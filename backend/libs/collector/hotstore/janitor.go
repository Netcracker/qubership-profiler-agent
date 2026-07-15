package hotstore

// The janitor keeps the hot store inside its retention and disk bounds
// (01-write-contract.md §4.6, §6.3, §3.5; 02-read-contract.md §4.2; 03 §3.9
// step 18). It never changes what the write path persists or how reads
// resolve — it only removes state whose durable copy is already in S3, plus
// the forced disk-budget eviction whose degradation the seal pass already
// knows how to record (trace_blob = NULL, truncated_reason = disk_budget).

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/pkg/errors"
)

// JanitorStats counts one JanitorPass's work (and, via
// JanitorCountersSnapshot, the process lifetime — the Prometheus seam, like
// UploadStats).
type JanitorStats struct {
	ParquetDeleted    int64
	PartitionsDropped int64
	WalsPurged        int64
	SegmentsEvicted   int64
	EvictedBytes      int64
}

// JanitorCountersSnapshot returns the process-lifetime janitor counters.
func (s *Store) JanitorCountersSnapshot() JanitorStats {
	s.janitorMu.Lock()
	defer s.janitorMu.Unlock()
	return s.janitorCounters
}

func (s *Store) countJanitor(stats JanitorStats) {
	s.janitorMu.Lock()
	defer s.janitorMu.Unlock()
	s.janitorCounters.ParquetDeleted += stats.ParquetDeleted
	s.janitorCounters.PartitionsDropped += stats.PartitionsDropped
	s.janitorCounters.WalsPurged += stats.WalsPurged
	s.janitorCounters.SegmentsEvicted += stats.SegmentsEvicted
	s.janitorCounters.EvictedBytes += stats.EvictedBytes
}

// SegmentsDiskUsage reports the on-disk bytes of the hot-store segments, as
// last measured by the janitor's disk-budget walk, next to the configured
// budget. Zero until the first pass runs.
func (s *Store) SegmentsDiskUsage() (bytes, budget int64) {
	return s.segmentsDiskBytes.Load(), s.cfg.ChunksStagingMaxBytes
}

// EvictedChunkRefs reports how many in-RAM chunk-index entries point at
// evicted segments — memory the eviction cannot release until the
// memory-budget task lands (risk B-3). Measured by the janitor pass.
func (s *Store) EvictedChunkRefs() int64 {
	return s.evictedChunkRefs.Load()
}

// QuarantineStats surfaces the stuck-quarantine gauges (01 §8): quarantined
// parquet files and snapshot-blocked pod-restarts, with the oldest failure
// time of each.
func (s *Store) QuarantineStats() (QuarantineStats, error) {
	return s.db.QuarantineStats()
}

// walFileNames are the per-pod-restart WAL files the §3.9 step-18 purge owns.
var walFileNames = []string{"dictionary.wal", "params.wal", "suspend.wal", "calls.wal"}

// JanitorPass runs one janitor round at the given wall clock: aged local
// parquet, then the hot-index partitions it unblocks, then the WAL purge, then
// the disk-budget eviction. nowMs is a parameter so tests replay history
// deterministically, mirroring SealDue.
func (s *Store) JanitorPass(ctx context.Context, nowMs int64) (JanitorStats, error) {
	var stats JanitorStats
	defer func() { s.countJanitor(stats) }() // partial passes still count what they did
	if err := s.dropAgedParquet(ctx, nowMs, &stats); err != nil {
		return stats, err
	}
	if err := s.dropAgedPartitions(ctx, nowMs, &stats); err != nil {
		return stats, err
	}
	if err := s.purgeWals(ctx, nowMs, &stats); err != nil {
		return stats, err
	}
	if err := s.enforceDiskBudget(ctx, nowMs, &stats); err != nil {
		return stats, err
	}
	return stats, s.measureEvictedChunkRefs()
}

// dropAgedParquet implements 01-write-contract.md §6.3: a local parquet file
// whose upload confirmed more than HotRetention ago is deleted together with
// its parquet_local row. Pending and quarantined files have uploaded_at NULL
// and are never touched.
func (s *Store) dropAgedParquet(ctx context.Context, nowMs int64, stats *JanitorStats) error {
	aged, err := s.db.AgedUploadedParquet(nowMs, s.cfg.HotRetention.Milliseconds())
	if err != nil {
		return err
	}
	for _, f := range aged {
		if err := ctx.Err(); err != nil {
			return err
		}
		// File first, row second: a crash in between leaves a row whose file is
		// missing, which the next pass (or recovery) clears — never an untracked
		// file on the PV.
		if err := os.Remove(f.Path); err != nil && !os.IsNotExist(err) {
			log.Error(ctx, err, "janitor: cannot remove aged parquet %s; keeping its row", f.Path)
			continue
		}
		if err := s.db.DeleteParquetLocalRow(f.Path); err != nil {
			return err
		}
		stats.ParquetDeleted++
		log.Info(ctx, "janitor: deleted local parquet %s (uploaded %s ago)",
			f.S3Key, time.Duration(nowMs-*f.UploadedAtMs)*time.Millisecond)
	}
	return nil
}

// dropAgedPartitions removes call-index partitions whose every row is durable
// in S3 and out of the overlap window. The walk is oldest-first and stops at
// the first bucket that is not droppable — the contiguity barrier that keeps
// the hot window truthful: everything newer than hot_window_oldest is really
// in the hot index, so the query's cold cutoff (02 §4.3) never skips a bucket
// whose cold copy is not confirmed yet. In particular a quarantined upload
// pins its bucket AND every newer bucket in the hot tier.
func (s *Store) dropAgedPartitions(ctx context.Context, nowMs int64, stats *JanitorStats) error {
	buckets, err := s.db.Buckets()
	if err != nil {
		return err
	}
	bucketDoneMs := s.cfg.TimeBucket.Milliseconds() + s.cfg.TimeBucketGrace.Milliseconds()
	for _, bucket := range buckets {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.cfg.BucketStartMs(bucket)+bucketDoneMs > nowMs {
			break // the bucket can still take on-time calls
		}
		unsealed, err := s.bucketHasUnsealedCalls(bucket)
		if err != nil {
			return err
		}
		if unsealed {
			break // rows a future seal owes; nothing here is durable yet
		}
		// Any remaining parquet_local row blocks the drop: uploaded_at NULL is
		// not durable, and an uploaded row still inside HotRetention means the
		// bucket is inside the §4.3 overlap window (dropAgedParquet deletes the
		// row once the window closes).
		n, err := s.db.BucketParquetCount(s.cfg.BucketStartMs(bucket))
		if err != nil {
			return err
		}
		if n > 0 {
			break
		}
		path, err := s.db.DropPartition(bucket, nowMs)
		if err != nil {
			return err
		}
		for _, p := range []string{path, path + "-wal", path + "-shm"} {
			if p == "" {
				continue
			}
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				log.Warning(ctx, "janitor: cannot remove partition file %s: %v", p, err)
			}
		}
		stats.PartitionsDropped++
		log.Info(ctx, "janitor: dropped call partition of bucket %d", bucket)
	}
	return nil
}

// bucketHasUnsealedCalls reports whether any pod-restart holds indexed calls
// past its seal watermark in this bucket — the same check the seal loop runs.
func (s *Store) bucketHasUnsealedCalls(bucket int64) (bool, error) {
	maxOffsets, err := s.db.MaxWalOffsets(bucket)
	if err != nil {
		return false, err
	}
	for podRestart, maxOffset := range maxOffsets {
		watermark, err := s.db.SealWatermark(podRestart, bucket)
		if err != nil {
			return false, err
		}
		if maxOffset >= watermark {
			return true, nil
		}
	}
	return false, nil
}

// purgeWals implements 01-write-contract.md §3.5 / 03 §3.9 step 18: a closed
// pod-restart's WAL files are deleted once everything they could decode is
// durable in S3 (parquet uploaded, dictionary snapshot uploaded), nothing in
// the hot index references the pod-restart any more, and the hold-back grace
// has elapsed. The pod-restart's directory and in-RAM state go with the WALs:
// past this point every read of its data is served by the cold tier.
func (s *Store) purgeWals(ctx context.Context, nowMs int64, stats *JanitorStats) error {
	candidates, err := s.db.WalPurgeCandidates()
	if err != nil {
		return err
	}
	graceMs := s.cfg.WalPurgeGrace.Milliseconds()
	for _, c := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		base := c.ClosedAtMs
		if c.DictUploadedAtMs > base {
			base = c.DictUploadedAtMs
		}
		if base+graceMs > nowMs {
			continue
		}
		if pending, err := s.db.HasPendingParquet(c.PodRestart); err != nil || pending {
			if err != nil {
				return err
			}
			continue // an un-confirmed file may need a re-seal from calls.wal
		}
		if _, indexed, err := s.db.MaxCallsWalOffset(c.PodRestart); err != nil || indexed {
			if err != nil {
				return err
			}
			continue // hot partitions still serve these rows; wait for their drop
		}
		key, err := ParsePodRestartKey(c.PodRestart)
		if err != nil {
			log.Error(ctx, err, "janitor: skip WAL purge of unparseable pod_restart %q", c.PodRestart)
			continue
		}
		dir := key.dir(s.cfg.DataDir)
		for _, name := range walFileNames {
			if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
				return errors.Wrapf(err, "purge %s of %s", name, c.PodRestart)
			}
		}
		if err := s.db.SetWalsPurged(c.PodRestart, nowMs); err != nil {
			return err
		}
		// The remaining directory holds only fully-uploaded leftovers (swept or
		// evicted segments, empty stream dirs); recovery must not resurrect the
		// pod-restart from it. Segment catalog rows, all at refcount 0 here, are
		// removed by the uploader sweep, which tolerates the missing files.
		if err := os.RemoveAll(dir); err != nil {
			log.Warning(ctx, "janitor: cannot remove pod-restart dir %s: %v", dir, err)
		}
		removeEmptyParents(filepath.Join(s.cfg.DataDir, "pods"), filepath.Dir(dir))
		s.forgetPodRestart(key)
		stats.WalsPurged++
		log.Info(ctx, "janitor: purged WALs of %s (%s past full flush)",
			c.PodRestart, time.Duration(nowMs-base)*time.Millisecond)
	}
	return nil
}

// removeEmptyParents best-effort removes now-empty pod/service/namespace dirs
// up to (not including) root; os.Remove fails on non-empty dirs, which stops
// the walk.
func removeEmptyParents(root, dir string) {
	for dir != root && len(dir) > len(root) {
		if err := os.Remove(dir); err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

// forgetPodRestart releases the in-RAM state of a purged pod-restart.
func (s *Store) forgetPodRestart(key PodRestartKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pods, key.String())
}

// enforceDiskBudget implements the 01-write-contract.md §4.6 eviction: when
// the hot-store segment files exceed ChunksStagingMaxBytes on disk, delete
// segment files in the deterministic order refcount-0 first, then the oldest
// referenced ones, never a segment still open for writes. The catalog row
// stays with status 'evicted', so a call whose chunks lived there seals with
// trace_blob = NULL and truncated_reason = disk_budget — the janitor only
// creates the condition, the seal pass records it.
func (s *Store) enforceDiskBudget(ctx context.Context, nowMs int64, stats *JanitorStats) error {
	budget := s.cfg.ChunksStagingMaxBytes
	rows, err := s.db.SegmentsForBudget()
	if err != nil {
		return err
	}
	type candidate struct {
		row  SegmentRow
		size int64
	}
	var total int64
	var zeroRef, referenced []candidate
	for _, row := range rows {
		info, err := os.Stat(row.Path)
		if err != nil {
			continue // already gone; the sweep owns the stale row
		}
		size := info.Size()
		total += size
		if row.Status == "open" {
			continue // never evict under a live gzip writer
		}
		if row.Refcount == 0 {
			zeroRef = append(zeroRef, candidate{row, size})
		} else {
			referenced = append(referenced, candidate{row, size})
		}
	}
	if total <= budget {
		s.segmentsDiskBytes.Store(total)
		return nil
	}
	log.Warning(ctx, "janitor: hot-store segments hold %d bytes over the %d budget; evicting", total, budget)
	for _, c := range append(zeroRef, referenced...) {
		if total <= budget {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := os.Remove(c.row.Path); err != nil && !os.IsNotExist(err) {
			log.Error(ctx, err, "janitor: cannot evict segment %s", c.row.Path)
			continue
		}
		if err := s.db.MarkSegmentEvicted(c.row.PodRestart, c.row.Stream, c.row.RollingSeq, nowMs); err != nil {
			return err
		}
		total -= c.size
		stats.SegmentsEvicted++
		stats.EvictedBytes += c.size
		log.Warning(ctx, "janitor: evicted segment %s/%s/%d (%d bytes, refcount %d) under the disk budget",
			c.row.PodRestart, c.row.Stream, c.row.RollingSeq, c.size, c.row.Refcount)
	}
	s.segmentsDiskBytes.Store(total)
	return nil
}

// measureEvictedChunkRefs counts the in-RAM chunk-index entries whose trace
// segment was evicted (risk B-3): the seal and the trace endpoint tolerate
// them, but their memory is released only by the future memory-budget task,
// so the count must be visible before it becomes a problem. Runs inside the
// janitor pass because it takes the store and per-pod locks.
func (s *Store) measureEvictedChunkRefs() error {
	rows, err := s.db.EvictedSegmentKeys()
	if err != nil {
		return err
	}
	evicted := make(map[string]map[int]struct{})
	for _, row := range rows {
		if row.Stream != StreamTrace {
			continue // only trace segments carry chunk-index entries
		}
		seqs, ok := evicted[row.PodRestart]
		if !ok {
			seqs = map[int]struct{}{}
			evicted[row.PodRestart] = seqs
		}
		seqs[row.RollingSeq] = struct{}{}
	}

	s.mu.Lock()
	pods := make([]*PodRestart, 0, len(s.pods))
	for _, pr := range s.pods {
		pods = append(pods, pr)
	}
	s.mu.Unlock()

	var dangling int64
	for _, pr := range pods {
		seqs, ok := evicted[pr.Key.String()]
		if !ok {
			continue
		}
		pr.mu.Lock()
		for _, refs := range pr.chunks {
			for _, ref := range refs {
				if _, gone := seqs[ref.RollingSeq]; gone {
					dangling++
				}
			}
		}
		pr.mu.Unlock()
	}
	s.evictedChunkRefs.Store(dangling)
	return nil
}

// RunJanitorLoop polls JanitorPass until the context ends, mirroring
// RunSealLoop; the collect wiring starts it (03-lifecycle.md §3.10 step 21).
func (s *Store) RunJanitorLoop(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := s.JanitorPass(ctx, time.Now().UnixMilli()); err != nil && ctx.Err() == nil {
				log.Error(ctx, err, "janitor pass failed")
			}
		}
	}
}
