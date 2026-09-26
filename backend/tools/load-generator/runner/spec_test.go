package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const minimalSpec = `
run:
  name: t2-bytes
  testid: t2-bytes-test
endpoints:
  k6: http://localhost:6565
  vm: http://localhost:8429
  collector: http://localhost:8081
ramp:
  levels: [10, 20, 40]
  hold:
    plateau:
      series:
        ingest-bytes: sum(rate(profiler_ingest_bytes_total[1m]))
detectors:
  - name: ingest-paused
    kind: sticky-share
    share: 0.05
    query: max(profiler_backpressure_ingest_paused)
`

func writeSpec(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "spec.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return path
}

func TestLoadSpecDefaults(t *testing.T) {
	s, err := LoadSpec(writeSpec(t, minimalSpec))
	require.NoError(t, err)
	assert.Equal(t, "t2-bytes", s.Run.Name)
	assert.Equal(t, []int{10, 20, 40}, s.Ramp.Levels)
	assert.Equal(t, 3*time.Minute, s.Ramp.Confirm.Timeout.std())
	assert.Equal(t, 15*time.Minute, s.Ramp.Hold.Max.std())
	assert.Equal(t, 2*time.Minute, s.Ramp.Hold.Plateau.Window.std())
	assert.Equal(t, 0.05, s.Ramp.Hold.Plateau.SlopeTolerance)
	assert.Equal(t, []float64{0.7, 1.0}, s.Pprof.Points)
	assert.Equal(t, []string{"profile", "heap", "goroutine"}, s.Pprof.Profiles)
	assert.Equal(t, "runs", s.Outputs)
}

func TestLoadSpecIngestDefaults(t *testing.T) {
	s, err := LoadSpec(writeSpec(t, `
run: {name: t4, testid: t}
endpoints: {k6: a, vm: b, collector: c}
ramp:
  levels: [20]
  confirm:
    ingest: {bytesPerVU: 19650}
  hold:
    plateau:
      series:
        ingest-bytes: sum(rate(profiler_ingest_bytes_total[1m]))
`))
	require.NoError(t, err)
	ing := s.Ramp.Confirm.Ingest
	assert.Equal(t, 0.25, ing.Tolerance, "tolerance defaults to 0.25")
	assert.Equal(t, "sum(rate(profiler_ingest_bytes_total[1m]))", ing.Query,
		"query defaults to the ingest-bytes plateau series")
}

func TestLoadSpecIngestNeedsAQuery(t *testing.T) {
	body := `
run: {name: t2, testid: t}
endpoints: {k6: a, vm: b, collector: c}
ramp:
  levels: [10]
  confirm:
    ingest: {bytesPerVU: 100}
  hold: {plateau: {series: {x: q}}}
`
	_, err := LoadSpec(writeSpec(t, body))
	require.Error(t, err, "no ingest-bytes plateau series and no explicit query")
}

// TestSpecTemplatesLoad keeps every committed template in ../specs loadable:
// a template that stops parsing is a broken runbook.
func TestSpecTemplatesLoad(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "specs", "*.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no spec templates found next to the runner")
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			_, err := LoadSpec(path)
			assert.NoError(t, err)
		})
	}
}

func TestLoadSpecRejects(t *testing.T) {
	for name, mutate := range map[string]string{
		"missing testid": `
run: {name: t2}
endpoints: {k6: a, vm: b, collector: c}
ramp:
  levels: [1]
  hold: {plateau: {series: {x: q}}}
`,
		"non-increasing levels": `
run: {name: t2, testid: t}
endpoints: {k6: a, vm: b, collector: c}
ramp:
  levels: [10, 10]
  hold: {plateau: {series: {x: q}}}
`,
		"unknown detector kind": `
run: {name: t2, testid: t}
endpoints: {k6: a, vm: b, collector: c}
ramp:
  levels: [10]
  hold: {plateau: {series: {x: q}}}
detectors: [{name: d, kind: sometimes, query: q}]
`,
		"unknown field": `
run: {name: t2, testid: t}
endpoints: {k6: a, vm: b, collector: c}
rampp: {}
`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := LoadSpec(writeSpec(t, mutate))
			require.Error(t, err)
		})
	}
}

// minioLabelRefs matches a label name where a PromQL query or a Grafana
// legend names one: inside a grouping clause, as a selector matcher, or as
// a legend template.
var minioLabelRefs = []*regexp.Regexp{
	regexp.MustCompile(`\bby\s*\(([^)]*)\)`),
	regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)\s*(?:=~|!~|!=|=)\s*\\?"`),
	regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`),
}

// Every label that names an S3 operation on a cdt_minio_* series is spelled
// [s3.OperationLabel]; any other spelling groups every operation into one
// series or matches nothing. A legend line is checked with the query line
// above it, since the legend names the label the query grouped by.
func TestMinioQueriesUseMetricLabel(t *testing.T) {
	var files []string
	for _, pattern := range []string{"../specs/*.yaml", "../dashboards/*.json"} {
		matches, err := filepath.Glob(pattern)
		require.NoError(t, err)
		files = append(files, matches...)
	}
	require.NotEmpty(t, files)

	checked := 0
	for _, file := range files {
		body, err := os.ReadFile(file)
		require.NoError(t, err)
		lines := strings.Split(string(body), "\n")
		for i, line := range lines {
			if !strings.Contains(line, "cdt_minio_") {
				continue
			}
			text := line
			if i+1 < len(lines) && strings.Contains(lines[i+1], "legendFormat") {
				text += "\n" + lines[i+1]
			}
			checked++
			for _, label := range operationLabels(text) {
				assert.Equal(t, s3.OperationLabel, label, "%s:%d: %s", file, i+1, strings.TrimSpace(line))
			}
		}
	}
	assert.NotZero(t, checked, "no cdt_minio_ query found in %v", files)
}

// operationLabels returns every label name in text that starts with
// "operation".
func operationLabels(text string) []string {
	var out []string
	for _, re := range minioLabelRefs {
		for _, m := range re.FindAllStringSubmatch(text, -1) {
			for _, name := range strings.Split(m[1], ",") {
				name = strings.TrimSpace(name)
				if strings.HasPrefix(name, "operation") {
					out = append(out, name)
				}
			}
		}
	}
	return out
}
