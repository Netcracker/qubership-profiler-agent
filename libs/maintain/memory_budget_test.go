package maintain

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestDefaultMaxGroupBytes pins the compaction budget default at 96 MiB. The
// OOM fix (QA 708#4) lowered it from 256 MiB so that
// MemoryBudgetMultiplier * MaxGroupBytes fits a small maintain container; the
// value is 1.5x the 64 MB seal cap so a lone at-cap sealed file stays under
// budget (compact.go splitByBudget). Reverting the default trips this test.
func TestDefaultMaxGroupBytes(t *testing.T) {
	assert.Equal(t, int64(96<<20), Config{}.Normalize().MaxGroupBytes)
}

// TestShippedMemoryBudgetInvariant checks that every values file pinning a
// maintain memory limit clears MemoryBudgetMultiplier * the default
// MaxGroupBytes. That invariant is what keeps a real compaction pass from
// OOM-killing (QA 708#4): the limit that equaled MaxGroupBytes left no room
// for the two in-memory copies of the compacted body plus GC churn.
func TestShippedMemoryBudgetInvariant(t *testing.T) {
	floor := int64(MemoryBudgetMultiplier) * Config{}.Normalize().MaxGroupBytes
	for _, path := range []string{
		"../../deploy/values-kind.yaml",
		"../../charts/profiler-backend/values.yaml",
	} {
		limit := maintainMemLimitBytes(t, path)
		assert.GreaterOrEqualf(t, limit, floor,
			"%s: maintain memory limit %d B must be >= %d B (%d x MaxGroupBytes)",
			path, limit, floor, MemoryBudgetMultiplier)
	}
}

// maintainMemLimitBytes reads maintain.resources.limits.memory from a helm
// values file and resolves it to bytes.
func maintainMemLimitBytes(t *testing.T, path string) int64 {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var values struct {
		Maintain struct {
			Resources struct {
				Limits struct {
					Memory string `yaml:"memory"`
				} `yaml:"limits"`
			} `yaml:"resources"`
		} `yaml:"maintain"`
	}
	require.NoError(t, yaml.Unmarshal(data, &values))
	mem := values.Maintain.Resources.Limits.Memory
	require.NotEmptyf(t, mem, "%s pins no maintain memory limit", path)
	return parseQuantityBytes(t, mem)
}

// parseQuantityBytes parses the subset of Kubernetes quantity suffixes the
// values files use: binary (Ki, Mi, Gi, Ti) and decimal (k, M, G, T), plus a
// plain byte count.
func parseQuantityBytes(t *testing.T, q string) int64 {
	t.Helper()
	binary := map[string]int64{"Ki": 1 << 10, "Mi": 1 << 20, "Gi": 1 << 30, "Ti": 1 << 40}
	decimal := map[string]int64{"k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12}
	for suffix, unit := range binary {
		if num, ok := strings.CutSuffix(q, suffix); ok {
			return mustAtoi(t, num) * unit
		}
	}
	for suffix, unit := range decimal {
		if num, ok := strings.CutSuffix(q, suffix); ok {
			return mustAtoi(t, num) * unit
		}
	}
	return mustAtoi(t, q)
}

func mustAtoi(t *testing.T, s string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	require.NoErrorf(t, err, "parse quantity %q", s)
	return n
}
