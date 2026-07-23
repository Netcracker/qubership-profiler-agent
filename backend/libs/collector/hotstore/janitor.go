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
	"sort"
	"strings"
	"sync/atomic"
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
	// WalsFastPurged counts the WalsPurged subset that took the near-empty
	// fast path (03 §3.9): purged after the grace without waiting for the
	// call-index partition drop.
	WalsFastPurged  int64
	SegmentsEvicted int64
	EvictedBytes    int64
	// QuarantineDropped counts quarantined parquet files removed by the
	// age/size cap (№2) — bounded, loudly-logged data loss.
	QuarantineDropped int64
	// DictionariesUnloaded / ChunkIndexesReleased count the mem-budget
	// evictions of closed pod-restarts' in-RAM state (№1).
	DictionariesUnloaded int64
	ChunkIndexesReleased int64
	// MemPressureSeals counts pod-restart buckets the mem budget early-sealed
	// (01 §6.1 trigger 3): closed-state eviction alone did not fit the budget,
	// so the oldest unsealed bucket was flushed to unpin its chunk indexes.
	MemPressureSeals int64
	// OrphanParquetRemoved counts sealed files with no catalog row — a crash
	// between the seal rename and its commit — removed by the janitor sweep
	// (re-review finding 7); their rows re-seal from the watermark.
	OrphanParquetRemoved int64
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
	s.janitorCounters.WalsFastPurged += stats.WalsFastPurged
	s.janitorCounters.SegmentsEvicted += stats.SegmentsEvicted
	s.janitorCounters.EvictedBytes += stats.EvictedBytes
	s.janitorCounters.QuarantineDropped += stats.QuarantineDropped
	s.janitorCounters.DictionariesUnloaded += stats.DictionariesUnloaded
	s.janitorCounters.ChunkIndexesReleased += stats.ChunkIndexesReleased
	s.janitorCounters.MemPressureSeals += stats.MemPressureSeals
	s.janitorCounters.OrphanParquetRemoved += stats.OrphanParquetRemoved
}

// SegmentsDiskUsage reports the on-disk bytes of the hot-store segments, as
// last measured by the janitor's disk-budget walk, next to the configured
// budget. Zero until the first pass runs.
func (s *Store) SegmentsDiskUsage() (bytes, budget int64) {
	return s.segmentsDiskBytes.Load(), s.cfg.ChunksStagingMaxBytes
}

// EvictedChunkRefs reports how many in-RAM chunk-index entries point at
// evicted segments (risk B-3), as measured by the janitor pass. The
// mem-budget eviction (№1) releases them together with the chunk index of a
// closed, fully-sealed pod-restart; refs held by live connections stay
// counted until their calls seal.
func (s *Store) EvictedChunkRefs() int64 {
	return s.evictedChunkRefs.Load()
}

// QuarantineStats surfaces the stuck-quarantine gauges (01 §8): quarantined
// parquet files, with the oldest failure time.
func (s *Store) QuarantineStats() (QuarantineStats, error) {
	return s.db.QuarantineStats()
}

// UploadBacklog surfaces the pending-upload gauges: the number of sealed files
// still owed to S3 and the sealed_at of the oldest, for upload_backlog and
// upload_lag_seconds. oldestSealedMs is nil when the queue is empty.
func (s *Store) UploadBacklog() (count int64, oldestSealedMs *int64, err error) {
	return s.db.UploadBacklog()
}

// walFileNames are the per-pod-restart WAL files the §3.9 step-18 purge owns.
var walFileNames = []string{"dictionary.wal", "params.wal", "suspend.wal", "calls.wal"}

// JanitorPass runs one janitor round at the given wall clock: aged local
// parquet, then the hot-index partitions it unblocks, then the WAL purge, then
// the disk-budget eviction, the quarantine cap, and the mem-budget eviction.
// nowMs is a parameter so tests replay history deterministically, mirroring
// SealDue.
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
	if err := s.capQuarantine(ctx, nowMs, &stats); err != nil {
		return stats, err
	}
	if err := s.sweepOrphanParquet(ctx, nowMs, &stats); err != nil {
		return stats, err
	}
	if err := s.enforceMemBudget(ctx, &stats); err != nil {
		return stats, err
	}
	if err := s.measureEvictedChunkRefs(); err != nil {
		return stats, err
	}
	// Everything above can change the pending backlog (dropped partitions,
	// dropped quarantine rows), so the №2 gates recompute last.
	return stats, s.refreshBackpressure(ctx)
}

// refreshBackpressure recomputes the №2 pending backlog — sealed parquet
// still owed to S3, the live call-index partitions on disk, and the tracked
// pod-restarts' WAL files (re-review finding 4) — and flips the two gates:
// the seal loop pauses once the pending parquet alone reaches half the budget
// (the data stays in WALs and segments, producing no new pending parquet),
// and ingest refuses RCV_DATA with ACK_ERROR before writing once the whole
// backlog reaches the full budget. That refusal is data loss, stated honestly
// (finding 1): the agent treats ACK_ERROR as fatal, drops the unacknowledged
// window, and reconnects as a fresh pod-restart (06 §6) — a bounded, counted
// loss (ingest_refused_bytes_total) instead of the PV running to ENOSPC.
// Called by recovery, the janitor pass, the seal loop, and the upload pass,
// so the gates lift promptly once S3 drains.
func (s *Store) refreshBackpressure(ctx context.Context) error {
	pending, err := s.db.PendingParquetBytes()
	if err != nil {
		return err
	}
	paths, err := s.db.PartitionPaths()
	if err != nil {
		return err
	}
	var partitions int64
	for _, p := range paths {
		for _, f := range []string{p, p + "-wal"} {
			if info, err := os.Stat(f); err == nil {
				partitions += info.Size()
			}
		}
	}
	walBytes := s.walBytesOnDisk()
	s.pendingParquetBytes.Store(pending)
	s.partitionsDiskBytes.Store(partitions)
	s.walDiskBytes.Store(walBytes)
	budget := s.cfg.PendingUploadMaxBytes
	// The seal gate reads ONLY the pending parquet share: sealing is what
	// grows it, and the upload loop drains it independently of sealing. Were
	// the partitions counted here too, a paused seal would pin its unsealed
	// partitions, which would keep the gate tripped — a deadlock. The ingest
	// gate reads the whole backlog — WAL files included, because agent data
	// grows the partitions and the WALs whether or not sealing runs, and the
	// WALs purge only after upload + retention + grace.
	backlog := pending + partitions + walBytes
	s.setGate(ctx, &s.sealPaused, pending >= budget/2, "seal", pending, budget/2)
	s.setGate(ctx, &s.ingestPaused, backlog >= budget, "ingest", backlog, budget)
	return nil
}

// setGate flips one backpressure gate, logging only the transitions.
func (s *Store) setGate(ctx context.Context, gate *atomic.Bool, engaged bool, name string, total, budget int64) {
	if gate.Swap(engaged) == engaged {
		return
	}
	if engaged {
		log.Warning(ctx, "backpressure: %s paused — pending backlog holds %d bytes against the %d budget", name, total, budget)
	} else {
		log.Info(ctx, "backpressure: %s resumed (%d of %d budget)", name, total, budget)
	}
}

// capQuarantine bounds the upload-failed/ quarantine (№2). Quarantined
// parquet past QuarantineMaxAge — or the oldest of it while the total exceeds
// QuarantineMaxBytes — is dropped together with its parquet_local row, which
// releases the segment refcounts it pinned and lets the WAL purge proceed;
// the loss is bounded and logged.
func (s *Store) capQuarantine(ctx context.Context, nowMs int64, stats *JanitorStats) error {
	maxAgeMs := s.cfg.QuarantineMaxAge.Milliseconds()
	rows, err := s.db.QuarantinedParquet()
	if err != nil {
		return err
	}
	sizes := make([]int64, len(rows))
	var total int64
	for i, f := range rows {
		if info, err := os.Stat(f.Path); err == nil {
			sizes[i] = info.Size()
		}
		total += sizes[i]
	}
	uploadFailedDir := filepath.Join(s.cfg.DataDir, "upload-failed")
	for i, f := range rows { // oldest FIRST failure first: the size cap evicts oldest
		if err := ctx.Err(); err != nil {
			return err
		}
		// The age cap keys off the immutable first-failure time, not the
		// re-test-reset upload_failed_at: otherwise the slow re-test refreshes the
		// timestamp every interval and the age arm never fires (a stuck bundle
		// would pin the PV until only the byte cap trips, with no age signal).
		firstFailed := f.UploadFailedAtMs
		if f.FirstFailedAtMs != nil {
			firstFailed = f.FirstFailedAtMs
		}
		overAge := firstFailed != nil && *firstFailed+maxAgeMs <= nowMs
		if !overAge && total <= s.cfg.QuarantineMaxBytes {
			break // rows are ordered by first-failure time; nothing later is older
		}
		if err := os.Remove(f.Path); err != nil && !os.IsNotExist(err) {
			log.Error(ctx, err, "janitor: cannot drop quarantined parquet %s; keeping its row", f.Path)
			continue
		}
		if err := s.db.DropParquetLocal(f.Path); err != nil {
			return err
		}
		total -= sizes[i]
		stats.QuarantineDropped++
		// A manifest-quarantined bundle still lives in the data layout (its
		// parquet is durable in S3); a parquet-quarantined one moved under
		// upload-failed/. The parquet loss differs: manifest-flavor keeps the
		// calls in the cold tier but loses their readable identity, so an
		// operator is not sent chasing lost call data during an incident.
		if strings.HasPrefix(f.Path, uploadFailedDir) {
			log.Warning(ctx, "janitor: dropped quarantined parquet %s (%d rows) past the quarantine cap — these calls are lost to the cold tier",
				f.Path, f.RowCount)
		} else {
			log.Warning(ctx, "janitor: dropped manifest-quarantined parquet %s (%d rows) past the quarantine cap — its calls remain in the cold tier but can no longer be named (identity lost)",
				f.Path, f.RowCount)
		}
	}
	return nil
}

// enforceMemBudget implements PROFILER_MEM_BUDGET (01 §4.6, №1): when the
// in-RAM pod-restart state exceeds the budget, closed pod-restarts are
// evicted oldest first — the dictionary maps always (they reload from the
// WAL), the chunk index once every indexed call is sealed (hot trace reads
// of those rows then fall through to the cold tier). Live connections are
// never touched. When that is not enough, the oldest unsealed bucket is
// early-sealed (01 §6.1 trigger 3) so the chunk indexes it pins become
// releasable — an unsealed hot bucket no longer wedges the budget.
func (s *Store) enforceMemBudget(ctx context.Context, stats *JanitorStats) error {
	budget := s.cfg.MemBudgetBytes
	s.mu.Lock()
	pods := make([]*PodRestart, 0, len(s.pods))
	for _, pr := range s.pods {
		pods = append(pods, pr)
	}
	s.mu.Unlock()

	var total int64
	for _, pr := range pods {
		total += pr.memFootprint()
	}
	if total <= budget {
		s.inRamBytes.Store(total)
		return nil
	}

	closed := make([]*PodRestart, 0, len(pods))
	for _, pr := range pods {
		if pr.Closed() {
			closed = append(closed, pr)
		}
	}
	sort.Slice(closed, func(i, j int) bool {
		return closed[i].Key.RestartTimeMs < closed[j].Key.RestartTimeMs
	})
	release := func() error {
		for _, pr := range closed {
			if total <= budget {
				return nil
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			before := pr.memFootprint()
			if pr.unloadDictionary() {
				stats.DictionariesUnloaded++
			}
			unsealed, err := s.db.HasUnsealedCalls(pr.Key.String())
			if err != nil {
				return err
			}
			if !unsealed && pr.releaseChunkIndex() {
				stats.ChunkIndexesReleased++
				log.Warning(ctx, "mem budget: released the chunk index of %v; hot trace reads of its rows fall through to the cold tier", pr.Key)
			}
			total -= before - pr.memFootprint()
		}
		return nil
	}
	if err := release(); err != nil {
		return err
	}
	if total > budget {
		// Trigger 3: flush the oldest dirty bucket, then re-try the release —
		// a chunk index an unsealed call pinned is now releasable.
		sealedPods, err := s.sealOldestUnsealedBucket(ctx)
		if err != nil {
			return err
		}
		if sealedPods > 0 {
			stats.MemPressureSeals += int64(sealedPods)
			if err := release(); err != nil {
				return err
			}
		}
	}
	s.inRamBytes.Store(total)
	if total > budget {
		log.Warning(ctx, "mem budget: %d bytes still held against the %d budget after evicting closed pod-restarts; live connections hold the rest", total, budget)
	}
	return nil
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
// durable in S3 (every sealed file uploaded — the sealed rows carry their own
// dictionary subset, №3), nothing in the hot index references the pod-restart
// any more, and the hold-back grace has elapsed. The pod-restart's directory
// and in-RAM state go with the WALs: past this point every read of its data
// is served by the cold tier.
//
// A near-empty pod-restart (directory at or under WalPurgeFastMaxBytes) takes
// the 03 §3.9 fast path instead of waiting for its partition drop, which
// bounds the tracked backlog under a reconnect storm at the restart rate times
// the grace (load-testing-report.md §8). Its index rows stay in the live
// partitions until the regular drop, so before the dictionary WAL goes the
// purge materializes the rows' method names into method_text; trace reads of
// those rows fall through to the cold tier exactly as after a mem-budget
// chunk-index release. Deleting the segments under the still-indexed rows is
// safe because the two gates above already ran: no pending parquet means every
// sealed file is confirmed in S3 and MarkUploaded released its segment
// refcounts, and no unsealed calls means no future seal will read them.
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
		if base+graceMs > nowMs {
			continue
		}
		if pending, err := s.db.HasPendingParquet(c.PodRestart); err != nil || pending {
			if err != nil {
				return err
			}
			continue // an un-confirmed file may need a re-seal from calls.wal
		}
		key, err := ParsePodRestartKey(c.PodRestart)
		if err != nil {
			log.Error(ctx, err, "janitor: skip WAL purge of unparseable pod_restart %q", c.PodRestart)
			continue
		}
		dir := key.dir(s.cfg.DataDir)
		fast := false
		if _, indexed, err := s.db.MaxCallsWalOffset(c.PodRestart); err != nil || indexed {
			if err != nil {
				return err
			}
			// Hot partitions still serve these rows; the regular path waits for
			// their drop, the fast path frees the WAL set under them.
			ok, err := s.fastPurge(ctx, key, c.PodRestart, dir)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			fast = true
		}
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
		if fast {
			stats.WalsFastPurged++
			log.Info(ctx, "janitor: fast-purged WALs of near-empty %s (%s past full flush; its rows stay hot until the partition drop)",
				c.PodRestart, time.Duration(nowMs-base)*time.Millisecond)
		} else {
			log.Info(ctx, "janitor: purged WALs of %s (%s past full flush)",
				c.PodRestart, time.Duration(nowMs-base)*time.Millisecond)
		}
	}
	return nil
}

// fastPurge decides the 03 §3.9 fast path for one still-indexed candidate and
// prepares its rows: eligible when the fast path is enabled, the pod-restart's
// directory is at or under the near-empty floor, and every indexed call is
// sealed (an in-flight seal pass keeps its watermark uncommitted, so
// HasUnsealedCalls also defers to the next janitor tick). On eligibility it
// backfills method_text — committed to the partitions BEFORE the caller
// deletes the dictionary WAL, so a crashed purge re-runs as a no-op.
func (s *Store) fastPurge(ctx context.Context, key PodRestartKey, podRestart, dir string) (bool, error) {
	if s.cfg.WalPurgeFastMaxBytes <= 0 {
		return false, nil
	}
	size, err := dirSizeBytes(dir)
	if err != nil {
		return false, err
	}
	if size > s.cfg.WalPurgeFastMaxBytes {
		return false, nil
	}
	if unsealed, err := s.db.HasUnsealedCalls(podRestart); err != nil || unsealed {
		return false, err
	}
	resolve := func(int) (string, bool) { return "", false }
	if pr, tracked := s.PodRestart(key); tracked {
		resolve = pr.DictWord // lazy-reloads from dictionary.wal if unloaded
	}
	unresolved, err := s.db.BackfillMethodText(podRestart, resolve)
	if err != nil {
		return false, err
	}
	if unresolved > 0 {
		log.Warning(ctx, "janitor: fast purge of %s leaves %d hot rows without a method name (dictionary incomplete); they render as #<id> until the partition drop",
			podRestart, unresolved)
	}
	return true, nil
}

// dirSizeBytes sums the file sizes under dir; a missing dir is zero (a crashed
// purge already removed it).
func dirSizeBytes(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil //nolint:nilerr // best-effort walk; a vanished entry is not a failure
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return 0, errors.Wrapf(err, "measure %s", dir)
	}
	return total, nil
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

// forgetPodRestart releases the in-RAM state of a purged pod-restart, and
// the service's intern pool with it once no pod-restart references it (№1).
func (s *Store) forgetPodRestart(key PodRestartKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pods, key.String())
	s.dropInternPoolLocked(key.Service)
}

// enforceDiskBudget implements the 01-write-contract.md §4.6 eviction: when
// the hot-store segment files exceed ChunksStagingMaxBytes on disk, delete
// segment files in a deterministic tier order — refcount-0 segments of
// fully-sealed CLOSED pod-restarts first, then the referenced ones, and the
// segments a future read still needs strictly LAST: pod-restarts with
// unsealed calls (№7, the HasUnsealedCalls check mirroring the deletion
// sweep — a seal that has not run yet will read those segments, the trace
// chains AND the sql/xml values, whose refcount rises only when the seal
// commits) and pod-restarts whose connection is still LIVE (re-review
// finding 5 — the agent deduplicates sql/xml values per connection, so a
// PARAM_BIG_DEDUP tag can reference a value sent hours ago, and a long
// call's trace chunks sit in long-rotated segments before its Call record
// exists; both look refcount-0 until the referencing call seals). A segment
// still open for writes is never evicted. The catalog row of an evicted
// segment stays with status 'evicted', so a call whose chunks lived there
// seals with trace_blob = NULL and truncated_reason = disk_budget — the
// janitor only creates the condition, the seal pass records it.
func (s *Store) enforceDiskBudget(ctx context.Context, nowMs int64, stats *JanitorStats) error {
	budget := s.cfg.ChunksStagingMaxBytes
	rows, err := s.db.SegmentsForBudget()
	if err != nil {
		return err
	}
	unsealed, err := s.db.UnsealedPodRestarts()
	if err != nil {
		return err
	}
	live, err := s.db.LivePodRestarts()
	if err != nil {
		return err
	}
	type candidate struct {
		row  SegmentRow
		size int64
	}
	var total int64
	var zeroRef, referenced, owedSeal []candidate
	for _, row := range rows {
		info, err := os.Stat(row.Path)
		if err != nil {
			continue // already gone; the sweep owns the stale row
		}
		size := info.Size()
		total += size
		switch {
		case row.Status == "open":
			// never evict under a live gzip writer
		case unsealed[row.PodRestart] || live[row.PodRestart]:
			owedSeal = append(owedSeal, candidate{row, size})
		case row.Refcount == 0:
			zeroRef = append(zeroRef, candidate{row, size})
		default:
			referenced = append(referenced, candidate{row, size})
		}
	}
	if total <= budget {
		s.segmentsDiskBytes.Store(total)
		return nil
	}
	log.Warning(ctx, "janitor: hot-store segments hold %d bytes over the %d budget; evicting", total, budget)
	for _, c := range append(append(zeroRef, referenced...), owedSeal...) {
		if total <= budget {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		// The candidate list may predate a seal commit that pinned the
		// segment; re-check the live catalog row before the unlink (№7).
		refcount, status, err := s.db.SegmentRefcount(c.row.PodRestart, c.row.Stream, c.row.RollingSeq)
		if err != nil {
			return err
		}
		if status != c.row.Status || refcount != c.row.Refcount {
			continue // stale snapshot; the next pass re-classifies it
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
		switch {
		case unsealed[c.row.PodRestart]:
			log.Warning(ctx, "janitor: evicted segment %s/%s/%d (%d bytes) that an OWED SEAL still needed — its calls will seal truncated (disk_budget)",
				c.row.PodRestart, c.row.Stream, c.row.RollingSeq, c.size)
		case live[c.row.PodRestart]:
			log.Warning(ctx, "janitor: evicted segment %s/%s/%d (%d bytes) of a LIVE pod-restart — a later dedup reference or long call loses it (disk_budget)",
				c.row.PodRestart, c.row.Stream, c.row.RollingSeq, c.size)
		default:
			log.Warning(ctx, "janitor: evicted segment %s/%s/%d (%d bytes, refcount %d) under the disk budget",
				c.row.PodRestart, c.row.Stream, c.row.RollingSeq, c.size, c.row.Refcount)
		}
	}
	s.segmentsDiskBytes.Store(total)
	return nil
}

// orphanSealedMinAge guards the orphan sweep against an in-flight seal pass:
// writeClassFile renames each class file to its sealed name BEFORE the pass
// commits (seal.go), so a catalog-less file may simply be mid-commit. The
// rename-to-commit window spans at most the remaining class writes of one
// pass — seconds — so files younger than this are left alone.
const orphanSealedMinAge = 10 * time.Minute

// sweepOrphanParquet removes sealed parquet files with no parquet_local row —
// a crash between the №6 rename and CommitSealPass (re-review finding 7).
// Such a file is invisible to the pending backlog and to every janitor step
// keyed off the catalog, so before this sweep it consumed PV until the next
// restart's recovery. The rows re-seal from the watermark, normally under the
// same deterministic name; a dictionary grown between the passes can shift a
// row's class, which is why the orphan cannot simply be adopted. Recovery
// runs the same walk without the age guard (recovery.go), because no seal is
// in flight at startup.
func (s *Store) sweepOrphanParquet(ctx context.Context, nowMs int64, stats *JanitorStats) error {
	s.sealPairMu.Lock()
	sealing := len(s.sealingPairs)
	s.sealPairMu.Unlock()
	if sealing > 0 {
		return nil // an in-flight pass may have renamed files it has not committed yet
	}
	paths, err := s.db.ParquetLocalPaths()
	if err != nil {
		return err
	}
	catalogued := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		catalogued[p] = struct{}{}
	}
	minAgeMs := orphanSealedMinAge.Milliseconds()
	root := filepath.Join(s.cfg.DataDir, "parquet")
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".parquet") {
			return nil //nolint:nilerr // a missing root means nothing sealed yet
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if _, ok := catalogued[path]; ok {
			return nil
		}
		info, statErr := d.Info()
		if statErr != nil || nowMs-info.ModTime().UnixMilli() < minAgeMs {
			return nil // young enough to be a pass racing this walk
		}
		if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
			log.Error(ctx, rmErr, "janitor: cannot remove orphan sealed parquet %s", path)
			return nil
		}
		stats.OrphanParquetRemoved++
		log.Warning(ctx, "janitor: removed orphan sealed parquet %s (no catalog row — a crash between the seal rename and its commit); its rows re-seal from the watermark", path)
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "sweep orphan sealed parquet")
	}
	return nil
}

// measureEvictedChunkRefs counts the in-RAM chunk-index entries whose trace
// segment was evicted (risk B-3): the seal and the trace endpoint tolerate
// them, and the mem-budget eviction (enforceMemBudget) releases them for
// closed, fully-sealed pod-restarts — this gauge shows what remains. Runs
// inside the janitor pass because it takes the store and per-pod locks.
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
				s.janitorLoopErrors.Add(1)
				log.Error(ctx, err, "janitor pass failed")
			}
		}
	}
}
