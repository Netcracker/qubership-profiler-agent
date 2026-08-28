package maintain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatsIsNoteworthy(t *testing.T) {
	cases := []struct {
		name       string
		stats      Stats
		noteworthy bool
	}{
		{"zero value", Stats{}, false},
		{"only routine skip counters", Stats{SkippedSmallGroups: 3, SkippedUnsettled: 2, PendingDeleteGroups: 1}, false},
		{"a fresh compaction", Stats{CompactedGroups: 1}, true},
		{"deleted grace-expired inputs", Stats{DeletedInputFiles: 4}, true},
		{"TTL parquet deletions", Stats{TTLParquetDeleted: 1}, true},
		{"TTL manifest deletions", Stats{TTLManifestsDeleted: 1}, true},
		{"a logged error", Stats{Errors: 1}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.noteworthy, c.stats.isNoteworthy())
		})
	}
}
