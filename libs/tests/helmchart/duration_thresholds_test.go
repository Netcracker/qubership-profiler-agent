// Package helmchart renders the profiler-backend chart and asserts the
// cross-workload invariants a template review cannot see — currently the
// finding-9 guarantee that PROFILER_DURATION_THRESHOLDS reaches the collector
// and the query workloads from ONE values key, so their tier tables can never
// drift (a drift silently drops rows from cold /calls).
package helmchart

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// renderChart runs `helm template` over charts/profiler-backend, skipping the
// test when helm is not installed (CI covers it via `make helm-lint` too).
func renderChart(t *testing.T, extraArgs ...string) string {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed; the chart render checks run where it is")
	}
	chart, err := filepath.Abs(filepath.Join("..", "..", "..", "charts", "profiler-backend"))
	require.NoError(t, err)
	args := append([]string{
		"template", "render-test", chart,
		"--set", "s3.endpoint=http://s3.test:9000",
		"--set", "s3.auth.accessKey=test",
		"--set", "s3.auth.secretKey=test",
	}, extraArgs...)
	out, err := exec.Command(helm, args...).CombinedOutput()
	require.NoError(t, err, "helm template failed: %s", out)
	return string(out)
}

// TestDurationThresholdsRenderIntoBothWorkloads pins finding 9: the one
// retention.durationThresholds key must render the same
// PROFILER_DURATION_THRESHOLDS env into BOTH the collector StatefulSet and
// the query Deployment, and an unset key must leave the env out of both (the
// binaries then share the built-in tier-table defaults).
func TestDurationThresholdsRenderIntoBothWorkloads(t *testing.T) {
	const thresholds = "50ms,2s,20s"
	envPair := regexp.MustCompile(
		fmt.Sprintf(`(?m)^\s+- name: PROFILER_DURATION_THRESHOLDS\n\s+value: %q$`, thresholds))

	// Commas separate list items in --set, so the value escapes them.
	set := renderChart(t, "--set",
		"retention.durationThresholds="+strings.ReplaceAll(thresholds, ",", `\,`))
	assert.Len(t, envPair.FindAllString(set, -1), 2,
		"the shared key must render into exactly two workloads: collector and query")

	unset := renderChart(t)
	assert.NotContains(t, unset, "PROFILER_DURATION_THRESHOLDS",
		"an empty key keeps the built-in defaults in both binaries")
}
