package hotread

// SetStatsPageRows shrinks the stats walk's SQL page so a test can cross
// page boundaries with a few rows. It returns the restore function.
func SetStatsPageRows(n int) func() {
	old := statsPageRows
	statsPageRows = n
	return func() { statsPageRows = old }
}
