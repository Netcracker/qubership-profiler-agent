package actions

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
)

// CleanupResult summarizes how many files the cleanup removed and how many bytes were freed.
type CleanupResult struct {
	RemovedByAge   int
	RemovedByQuota int
	FreedBytes     int64
}

// CleanupStaleDumps enforces retention on pending dump artifacts:
//
//  1. Files whose mtime is older than maxAge are deleted, including the single
//     newest file. The age rule is absolute: "after maxAge, the file is gone".
//  2. Of what remains, if the combined size exceeds maxBytes, the oldest files
//     (by mtime) are deleted in order until the total is at or below the limit.
//     The newest remaining file is always kept, even if it alone exceeds maxBytes.
//
// Each deletion is logged at WARN level with filename, size, age, and reason.
// A value of 0 for maxAge or maxBytes disables that step.
func CleanupStaleDumps(
	ctx context.Context,
	patterns []string,
	maxAge time.Duration,
	maxBytes int64,
) CleanupResult {
	var result CleanupResult

	files := collectFiles(ctx, patterns)
	if len(files) == 0 {
		return result
	}

	now := time.Now()

	if maxAge > 0 {
		kept := files[:0]
		for _, f := range files {
			age := now.Sub(f.info.ModTime())
			if age > maxAge {
				if removeOne(ctx, f, age, "max_age_exceeded") {
					result.RemovedByAge++
					result.FreedBytes += f.info.Size()
				}
				continue
			}
			kept = append(kept, f)
		}
		files = kept
	}

	if maxBytes > 0 && len(files) > 1 {
		// Sort oldest first so we evict from the front until the newest remains.
		sort.SliceStable(files, func(i, j int) bool {
			return files[i].info.ModTime().Before(files[j].info.ModTime())
		})
		var total int64
		for _, f := range files {
			total += f.info.Size()
		}
		i := 0
		for total > maxBytes && len(files)-i > 1 {
			f := files[i]
			age := now.Sub(f.info.ModTime())
			if removeOne(ctx, f, age, "pending_quota_exceeded") {
				result.RemovedByQuota++
				result.FreedBytes += f.info.Size()
				total -= f.info.Size()
			}
			i++
		}
	}

	return result
}

type pendingFile struct {
	path string
	info os.FileInfo
}

// collectFiles expands glob patterns, stats each match, and returns
// unique regular files. Duplicates across overlapping patterns are dropped.
func collectFiles(ctx context.Context, patterns []string) []pendingFile {
	seen := make(map[string]struct{})
	var out []pendingFile
	for _, pattern := range patterns {
		matches, err := filepath.Glob(filepath.Clean(pattern))
		if err != nil {
			log.Errorf(ctx, err, "failed to glob pattern %q for cleanup", pattern)
			continue
		}
		for _, m := range matches {
			if _, dup := seen[m]; dup {
				continue
			}
			info, err := os.Stat(m)
			if err != nil {
				log.Errorf(ctx, err, "failed to stat %s during cleanup", m)
				continue
			}
			if !info.Mode().IsRegular() {
				continue
			}
			seen[m] = struct{}{}
			out = append(out, pendingFile{path: m, info: info})
		}
	}
	return out
}

func removeOne(ctx context.Context, f pendingFile, age time.Duration, reason string) bool {
	log.Warnf(ctx,
		"deleting stale dump file: %s (size=%d bytes, age=%s, reason=%s)",
		f.path, f.info.Size(), age.Truncate(time.Second), reason)
	if err := os.Remove(f.path); err != nil {
		log.Errorf(ctx, err, "failed to delete stale dump file %s", f.path)
		return false
	}
	return true
}
