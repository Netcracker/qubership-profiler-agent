package actions

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeFileWithSizeAndAge creates a file with exactly `size` bytes and sets its
// mtime to `now - age`. Returns the full path.
func writeFileWithSizeAndAge(t *testing.T, dir, name string, size int64, age time.Duration) string {
	t.Helper()
	path := filepath.Join(dir, name)
	content := make([]byte, size)
	require.NoError(t, os.WriteFile(path, content, 0644))
	when := time.Now().Add(-age)
	require.NoError(t, os.Chtimes(path, when, when))
	return path
}

func listRemaining(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

func TestCleanup_EmptyFolder(t *testing.T) {
	dir := t.TempDir()
	pattern := filepath.Join(dir, "*.hprof*")

	res := CleanupStaleDumps(testCtx, []string{pattern}, 48*time.Hour, 10<<30)

	assert.Equal(t, CleanupResult{}, res)
}

func TestCleanup_FreshBelowQuota(t *testing.T) {
	dir := t.TempDir()
	writeFileWithSizeAndAge(t, dir, "a.hprof", 100, time.Hour)
	writeFileWithSizeAndAge(t, dir, "b.hprof", 100, 2*time.Hour)

	pattern := filepath.Join(dir, "*.hprof*")
	res := CleanupStaleDumps(testCtx, []string{pattern}, 48*time.Hour, 10<<30)

	assert.Equal(t, 0, res.RemovedByAge)
	assert.Equal(t, 0, res.RemovedByQuota)
	assert.Equal(t, []string{"a.hprof", "b.hprof"}, listRemaining(t, dir))
}

func TestCleanup_SingleTooOld_IsDeleted(t *testing.T) {
	// Age rule must apply even to the newest (single) file — "after maxAge, it's gone".
	dir := t.TempDir()
	writeFileWithSizeAndAge(t, dir, "old.hprof", 100, 72*time.Hour)

	pattern := filepath.Join(dir, "*.hprof*")
	res := CleanupStaleDumps(testCtx, []string{pattern}, 48*time.Hour, 10<<30)

	assert.Equal(t, 1, res.RemovedByAge)
	assert.Equal(t, int64(100), res.FreedBytes)
	assert.Empty(t, listRemaining(t, dir))
}

func TestCleanup_MixedAges(t *testing.T) {
	dir := t.TempDir()
	writeFileWithSizeAndAge(t, dir, "fresh.hprof", 100, time.Hour)
	writeFileWithSizeAndAge(t, dir, "stale.hprof", 200, 72*time.Hour)

	pattern := filepath.Join(dir, "*.hprof*")
	res := CleanupStaleDumps(testCtx, []string{pattern}, 48*time.Hour, 10<<30)

	assert.Equal(t, 1, res.RemovedByAge)
	assert.Equal(t, int64(200), res.FreedBytes)
	assert.Equal(t, []string{"fresh.hprof"}, listRemaining(t, dir))
}

func TestCleanup_OverQuota_KeepsNewest(t *testing.T) {
	// Quota rule must never delete the single newest file, even if it alone exceeds the cap.
	dir := t.TempDir()
	writeFileWithSizeAndAge(t, dir, "a.hprof", 1000, 3*time.Hour)
	writeFileWithSizeAndAge(t, dir, "b.hprof", 1000, 2*time.Hour)
	writeFileWithSizeAndAge(t, dir, "c.hprof", 5000, time.Hour) // newest

	pattern := filepath.Join(dir, "*.hprof*")
	// Quota 100 bytes — everything except newest should go.
	res := CleanupStaleDumps(testCtx, []string{pattern}, 0, 100)

	assert.Equal(t, 0, res.RemovedByAge)
	assert.Equal(t, 2, res.RemovedByQuota)
	assert.Equal(t, int64(2000), res.FreedBytes)
	assert.Equal(t, []string{"c.hprof"}, listRemaining(t, dir))
}

func TestCleanup_OverQuota_LRUUntilUnderLimit(t *testing.T) {
	// Evict oldest first until total <= quota.
	dir := t.TempDir()
	writeFileWithSizeAndAge(t, dir, "a.hprof", 300, 4*time.Hour) // oldest
	writeFileWithSizeAndAge(t, dir, "b.hprof", 300, 3*time.Hour)
	writeFileWithSizeAndAge(t, dir, "c.hprof", 300, 2*time.Hour)
	writeFileWithSizeAndAge(t, dir, "d.hprof", 300, time.Hour) // newest

	pattern := filepath.Join(dir, "*.hprof*")
	// Total = 1200, quota = 700. Need to evict 500 bytes: "a" (300) then "b" (300) → 600 left, done.
	res := CleanupStaleDumps(testCtx, []string{pattern}, 0, 700)

	assert.Equal(t, 2, res.RemovedByQuota)
	assert.Equal(t, int64(600), res.FreedBytes)
	assert.Equal(t, []string{"c.hprof", "d.hprof"}, listRemaining(t, dir))
}

func TestCleanup_AgeThenSizeComposition(t *testing.T) {
	// Age runs first, then size.
	dir := t.TempDir()
	writeFileWithSizeAndAge(t, dir, "ancient.hprof", 500, 72*time.Hour) // killed by age
	writeFileWithSizeAndAge(t, dir, "old.hprof", 400, 5*time.Hour)      // killed by quota
	writeFileWithSizeAndAge(t, dir, "mid.hprof", 400, 3*time.Hour)      // survives
	writeFileWithSizeAndAge(t, dir, "new.hprof", 400, time.Hour)        // newest, survives

	pattern := filepath.Join(dir, "*.hprof*")
	res := CleanupStaleDumps(testCtx, []string{pattern}, 48*time.Hour, 1000)

	assert.Equal(t, 1, res.RemovedByAge)
	assert.Equal(t, 1, res.RemovedByQuota)
	assert.Equal(t, int64(900), res.FreedBytes)
	assert.Equal(t, []string{"mid.hprof", "new.hprof"}, listRemaining(t, dir))
}

func TestCleanup_HprofAndZip_CountedIndependently(t *testing.T) {
	// A .hprof and its matching .hprof.zip are independent files with their own mtime/size.
	// The zip is typically younger (created later from the raw dump).
	dir := t.TempDir()
	writeFileWithSizeAndAge(t, dir, "20260101T000000.hprof", 1_000_000, 10*time.Minute)
	writeFileWithSizeAndAge(t, dir, "20260101T000000.hprof.zip", 50_000, time.Minute)

	pattern := filepath.Join(dir, "*.hprof*")
	// Quota 100_000 — the raw .hprof (older, larger) must be dropped; the zip stays.
	res := CleanupStaleDumps(testCtx, []string{pattern}, 0, 100_000)

	assert.Equal(t, 1, res.RemovedByQuota)
	assert.Equal(t, int64(1_000_000), res.FreedBytes)
	assert.Equal(t, []string{"20260101T000000.hprof.zip"}, listRemaining(t, dir))
}

func TestCleanup_DisabledByZero(t *testing.T) {
	// maxAge=0 and maxBytes=0 disable their respective checks.
	dir := t.TempDir()
	writeFileWithSizeAndAge(t, dir, "ancient.hprof", 500, 365*24*time.Hour)
	writeFileWithSizeAndAge(t, dir, "huge.hprof", 1<<30, time.Minute)

	pattern := filepath.Join(dir, "*.hprof*")
	res := CleanupStaleDumps(testCtx, []string{pattern}, 0, 0)

	assert.Equal(t, CleanupResult{}, res)
	assert.Equal(t, []string{"ancient.hprof", "huge.hprof"}, listRemaining(t, dir))
}
