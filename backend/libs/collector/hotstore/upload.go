package hotstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/pkg/errors"
)

type (
	// ObjectStore is the narrow S3 surface the upload task needs: idempotent
	// PUTs at deterministic keys (01-write-contract.md §6.2, §6.6). Every
	// implementation must wrap a rejection that retrying cannot fix in
	// PermanentUploadError so the quarantine path (§8) can tell it apart from
	// a transient 5xx/network failure.
	ObjectStore interface {
		PutFile(ctx context.Context, key, localPath string) error
		PutBytes(ctx context.Context, key string, body []byte) error
	}

	// PermanentUploadError marks an S3 rejection no retry can fix (a 4xx): the
	// file moves to upload-failed/ for human attention and its segment
	// refcounts stay pinned (01-write-contract.md §8).
	PermanentUploadError struct{ Err error }

	// Uploader drains parquet_local to S3 and upserts the per-day pods/v1
	// identity manifests (01-write-contract.md §3.6, §6.2; 03-lifecycle.md
	// §3.8-§3.9). The dictionary and suspend snapshots are gone: sealed rows
	// are self-contained (№3, №23). One instance per collector process; Pass
	// is single-threaded.
	Uploader struct {
		store *Store
		s3    ObjectStore

		mu       sync.Mutex
		counters UploadStats

		// loopErrors counts failed upload passes at the pass-failed log site
		// (the upload_loop_errors_total seam). Per-file PUT failures are counted
		// separately by UploadStats.FailedPuts; this is a whole-pass failure.
		loopErrors atomic.Int64
	}

	// UploadStats counts one Pass's work (and, via CountersSnapshot, the
	// process lifetime — the Prometheus seam).
	UploadStats struct {
		UploadedFiles int64
		// FailedPuts counts every failed PUT attempt, transient or permanent;
		// its rate is the upload-failure alerting signal. RetriedPuts counts
		// only the attempts a retry followed.
		FailedPuts         int64
		RetriedPuts        int64
		QuarantinedFiles   int64
		QuarantinedObjects int64
		ManifestPuts       int64
		SegmentsDeleted    int64
		// RequeuedFiles counts the №2 quarantine re-tests: permanently-rejected
		// uploads put back into the queue after the retest interval.
		RequeuedFiles int64
	}

	// podsManifest recovers the readable pod-restart identity behind the
	// one-way <podRestartHash> of the parquet keys (01 §3.6).
	podsManifest struct {
		Namespace     string `json:"namespace"`
		Service       string `json:"service"`
		Pod           string `json:"pod"`
		RestartTimeMs int64  `json:"restart_time_ms"`
		TimerStartMs  int64  `json:"timer_start_ms"`
		Replica       string `json:"replica"`
		TimeMinMs     int64  `json:"time_min_ms"`
		TimeMaxMs     int64  `json:"time_max_ms"`
	}
)

func (e *PermanentUploadError) Error() string { return "permanent upload rejection: " + e.Err.Error() }
func (e *PermanentUploadError) Unwrap() error { return e.Err }

// IsPermanentUploadError classifies an ObjectStore error for the §8
// quarantine path.
func IsPermanentUploadError(err error) bool {
	var p *PermanentUploadError
	return errors.As(err, &p)
}

// NewUploader binds the store to an object store. The seal pass records every
// file's deterministic S3 key, so the uploader never derives keys itself.
func NewUploader(store *Store, s3 ObjectStore) *Uploader {
	return &Uploader{store: store, s3: s3}
}

// CountersSnapshot returns the process-lifetime upload metrics.
func (u *Uploader) CountersSnapshot() UploadStats {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.counters
}

// LoopErrors reports the process-lifetime count of failed upload passes (the
// upload_loop_errors_total seam), distinct from the per-file FailedPuts.
func (u *Uploader) LoopErrors() int64 { return u.loopErrors.Load() }

// add merges another stats bundle in; used by the pass accumulator and by
// the №25 upload workers merging their per-worker counts.
func (s *UploadStats) add(o UploadStats) {
	s.UploadedFiles += o.UploadedFiles
	s.FailedPuts += o.FailedPuts
	s.RetriedPuts += o.RetriedPuts
	s.QuarantinedFiles += o.QuarantinedFiles
	s.QuarantinedObjects += o.QuarantinedObjects
	s.ManifestPuts += o.ManifestPuts
	s.SegmentsDeleted += o.SegmentsDeleted
	s.RequeuedFiles += o.RequeuedFiles
}

func (u *Uploader) accumulate(stats UploadStats) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.counters.add(stats)
}

// Pass runs one upload round: the quarantine re-test, pending parquet files,
// then the refcount-0 segment sweep. Per-file failures are logged and left
// for the next pass; only a context or SQLite failure aborts the pass.
func (u *Uploader) Pass(ctx context.Context) (UploadStats, error) {
	var stats UploadStats
	defer func() { u.accumulate(stats) }()

	if err := u.requeueQuarantined(ctx, &stats); err != nil {
		return stats, err
	}
	if err := u.uploadPending(ctx, &stats); err != nil {
		return stats, err
	}
	if err := u.sweepSegments(ctx, &stats); err != nil {
		return stats, err
	}
	// Confirmed uploads shrink the pending backlog: lift the №2 gates now
	// rather than a janitor pass later.
	return stats, u.store.refreshBackpressure(ctx)
}

// requeueQuarantined implements the №2 slow re-test: quarantined parquet
// uploads whose last rejection is older than the retest interval re-enter
// the queue. A rejection that persists re-quarantines with a fresh
// timestamp, so the re-test costs one PUT per interval, not a hot loop.
func (u *Uploader) requeueQuarantined(ctx context.Context, stats *UploadStats) error {
	cutoffMs := time.Now().UnixMilli() - u.store.cfg.QuarantineRetestInterval.Milliseconds()
	files, err := u.store.db.RequeueQuarantinedParquet(cutoffMs)
	if err != nil {
		return err
	}
	stats.RequeuedFiles += files
	if files > 0 {
		log.Info(ctx, "upload: re-testing %d quarantined files", files)
	}
	return nil
}

// uploadPending drains parquet_local rows with uploaded_at IS NULL over a
// bounded worker pool (№25): one stuck PUT no longer head-of-line-blocks the
// backlog. Per file the order is fixed by invariant C1: PUT the object,
// refresh the day's pods manifest, and only then commit uploaded_at together
// with the refcount release — any failure before the commit leaves the row
// pending and the next pass redoes the idempotent PUTs.
func (u *Uploader) uploadPending(ctx context.Context, stats *UploadStats) error {
	files, err := u.store.db.PendingUploads()
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}
	workers := u.store.cfg.UploadConcurrency
	if workers > len(files) {
		workers = len(files)
	}

	var (
		mu            sync.Mutex // guards firstErr, manifestsDone, and the merge into stats
		firstErr      error
		manifestsDone = map[string]struct{}{}
	)
	jobs := make(chan ParquetLocalFile)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var local UploadStats
			for f := range jobs {
				mu.Lock()
				stop := firstErr != nil
				mu.Unlock()
				if stop || ctx.Err() != nil {
					continue // drain the channel; the pass is already failing
				}
				if err := u.uploadOne(ctx, f, &local, &mu, manifestsDone); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}
			mu.Lock()
			stats.add(local)
			mu.Unlock()
		}()
	}
	for _, f := range files {
		jobs <- f
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

// uploadOne runs the §6.2 sequence for one file. A per-file failure is
// logged and left for the next pass (nil); only a context or SQLite failure
// comes back as an error and fails the pass.
func (u *Uploader) uploadOne(ctx context.Context, f ParquetLocalFile, stats *UploadStats,
	manifestMu *sync.Mutex, manifestsDone map[string]struct{}) error {

	if _, err := os.Stat(f.Path); err != nil {
		// Recovery clears rows whose file is gone (03 §3.6); mid-flight the
		// pass just skips them.
		log.Warning(ctx, "upload: sealed parquet %s is missing on disk, skipping", f.Path)
		return nil
	}
	err := u.putWithRetry(ctx, f.S3Key, func() error { return u.s3.PutFile(ctx, f.S3Key, f.Path) }, stats)
	if IsPermanentUploadError(err) {
		if qErr := u.quarantine(ctx, f, err, stats); qErr != nil {
			log.Error(ctx, qErr, "upload: quarantine of %s failed; the file stays pending", f.Path)
		}
		return nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return err
		}
		log.Error(ctx, err, "upload: PUT %s failed after retries; will retry next pass", f.S3Key)
		return nil
	}
	// The manifest upsert is serialized across workers: manifestsDone dedups
	// per (pod-restart, day) and the PUT itself is idempotent, so coarse
	// serialization is correct and manifests are rare next to parquet PUTs.
	manifestMu.Lock()
	mErr := u.upsertManifest(ctx, f, manifestsDone, stats)
	manifestMu.Unlock()
	if mErr != nil {
		if ctx.Err() != nil {
			return mErr
		}
		// uploaded_at stays NULL, so the next pass re-runs the parquet PUT
		// and the manifest PUT; both are idempotent.
		log.Error(ctx, mErr, "upload: pods manifest for %s failed; %s stays pending", f.PodRestart, f.S3Key)
		return nil
	}
	if err := u.store.db.MarkUploaded(f.Path, time.Now().UnixMilli()); err != nil {
		return err
	}
	stats.UploadedFiles++
	log.Info(ctx, "uploaded %s (%d rows)", f.S3Key, f.RowCount)
	return nil
}

// putWithRetry runs one PUT with the §6.2 exponential backoff. A permanent
// rejection returns immediately; exhausting the in-pass attempts returns the
// last error and the file stays pending for the next pass.
func (u *Uploader) putWithRetry(ctx context.Context, key string, put func() error, stats *UploadStats) error {
	cfg := u.store.cfg
	delay := cfg.UploadRetryBaseDelay
	for attempt := 1; ; attempt++ {
		err := put()
		if err != nil {
			stats.FailedPuts++
		}
		if err == nil || IsPermanentUploadError(err) {
			return err
		}
		if attempt >= cfg.UploadRetryAttempts {
			return err
		}
		stats.RetriedPuts++
		log.Warning(ctx, "upload: PUT %s attempt %d/%d failed, retrying in %v: %v",
			key, attempt, cfg.UploadRetryAttempts, delay, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
}

// quarantine implements the §8 upload-failed/ path: the file moves out of the
// PV data layout, the parquet_local row follows it and leaves the upload
// queue, and — critically — its parquet_segments rows survive, so the segment
// refcounts stay pinned until a human resolves the rejection.
func (u *Uploader) quarantine(ctx context.Context, f ParquetLocalFile, cause error, stats *UploadStats) error {
	dir := filepath.Join(u.store.cfg.DataDir, "upload-failed")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return errors.Wrap(err, "create upload-failed dir")
	}
	dest := filepath.Join(dir, filepath.Base(f.Path))
	if err := os.Rename(f.Path, dest); err != nil {
		return errors.Wrap(err, "move rejected parquet")
	}
	if err := u.store.db.MarkUploadFailed(f.Path, dest, time.Now().UnixMilli()); err != nil {
		return err
	}
	stats.QuarantinedFiles++
	log.Error(ctx, cause, "upload: S3 rejected %s; moved to %s, segment refcounts stay pinned", f.S3Key, dest)
	return nil
}

// upsertManifest writes or refreshes the pods manifest of the file's UTC day
// (01 §3.6): idempotent per (day, pod-restart), bounds over every file the
// pod-restart sealed into that day so a later seal only widens time_max_ms.
// A permanently rejected manifest is quarantined and treated as done: the
// parquet object itself is already durable, so blocking its uploaded_at commit
// would re-PUT it forever for a manifest S3 refuses to take.
func (u *Uploader) upsertManifest(ctx context.Context, f ParquetLocalFile, done map[string]struct{}, stats *UploadStats) error {
	dayStartMs := utcDayStartMs(f.TimeBucketMs)
	doneKey := fmt.Sprintf("%s/%d", f.PodRestart, dayStartMs)
	if _, ok := done[doneKey]; ok {
		return nil
	}
	key, err := ParsePodRestartKey(f.PodRestart)
	if err != nil {
		return err
	}
	s3Key := path.Join("pods/v1", utcDayPath(dayStartMs), PodRestartHash(key)+".json")
	if u.objectQuarantined(s3Key) {
		done[doneKey] = struct{}{}
		return nil
	}
	timeMinMs, timeMaxMs, ok, err := u.store.db.ManifestBounds(f.PodRestart, dayStartMs, dayStartMs+dayMs)
	if err != nil {
		return err
	}
	if !ok {
		return errors.Errorf("no sealed files for %s on day %d despite pending upload", f.PodRestart, dayStartMs)
	}
	manifest := podsManifest{
		Namespace:     key.Namespace,
		Service:       key.Service,
		Pod:           key.PodName,
		RestartTimeMs: key.RestartTimeMs,
		TimerStartMs:  u.timerStartMs(ctx, key),
		Replica:       u.store.cfg.Replica,
		TimeMinMs:     timeMinMs,
		TimeMaxMs:     timeMaxMs,
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return errors.Wrap(err, "encode pods manifest")
	}
	if err := u.putWithRetry(ctx, s3Key, func() error { return u.s3.PutBytes(ctx, s3Key, body) }, stats); err != nil {
		if !IsPermanentUploadError(err) {
			return err
		}
		if qErr := u.quarantineObject(ctx, s3Key, body, err); qErr != nil {
			return qErr
		}
		stats.QuarantinedObjects++
		done[doneKey] = struct{}{}
		return nil
	}
	done[doneKey] = struct{}{}
	stats.ManifestPuts++
	return nil
}

// quarantineObject persists a permanently rejected snapshot or manifest body
// under upload-failed/<s3 key>, mirroring the parquet quarantine (01 §8): the
// bytes wait for a human instead of retrying forever. Idempotent — a repeat
// overwrites the same file.
func (u *Uploader) quarantineObject(ctx context.Context, s3Key string, body []byte, cause error) error {
	dest := filepath.Join(u.store.cfg.DataDir, "upload-failed", filepath.FromSlash(s3Key))
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return errors.Wrap(err, "create upload-failed dir")
	}
	if err := os.WriteFile(dest, body, 0o644); err != nil {
		return errors.Wrap(err, "write quarantined object")
	}
	log.Error(ctx, cause, "upload: S3 rejected %s permanently; body kept at %s, retry stopped", s3Key, dest)
	return nil
}

// objectQuarantined reports whether a key already sits in upload-failed/.
func (u *Uploader) objectQuarantined(s3Key string) bool {
	_, err := os.Stat(filepath.Join(u.store.cfg.DataDir, "upload-failed", filepath.FromSlash(s3Key)))
	return err == nil
}

// sweepSegments unlinks segments that are done: refcount 0 (every sealed row
// they source is uploaded) in a closed pod-restart with no un-sealed calls
// left (01 §4.4, 03 §3.7 step 14). A bare refcount 0 is NOT enough — a live
// or not-yet-sealed pod-restart's segments carry data future seals still owe.
func (u *Uploader) sweepSegments(ctx context.Context, stats *UploadStats) error {
	candidates, err := u.store.db.DeletableSegmentCandidates()
	if err != nil {
		return err
	}
	sealed := map[string]bool{}
	for _, seg := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, ok := sealed[seg.PodRestart]
		if !ok {
			unsealed, err := u.store.db.HasUnsealedCalls(seg.PodRestart)
			if err != nil {
				return err
			}
			done = !unsealed
			sealed[seg.PodRestart] = done
		}
		if !done {
			continue
		}
		// File first, row second: a crash in between leaves a stale catalog row
		// (harmless, re-swept later) rather than an untracked file on the PV.
		if err := os.Remove(seg.Path); err != nil && !os.IsNotExist(err) {
			log.Error(ctx, err, "upload: cannot remove segment %s; keeping its catalog row", seg.Path)
			continue
		}
		if err := u.store.db.DeleteSegment(seg.PodRestart, seg.Stream, seg.RollingSeq); err != nil {
			return err
		}
		stats.SegmentsDeleted++
		log.Info(ctx, "deleted segment %s/%s/%d after upload", seg.PodRestart, seg.Stream, seg.RollingSeq)
	}
	return nil
}

// timerStartMs resolves the pod-restart's trace epoch for the pods manifest;
// 0 when the pod-restart never sent a trace stream.
func (u *Uploader) timerStartMs(ctx context.Context, key PodRestartKey) int64 {
	pr, ok := u.store.PodRestart(key)
	if !ok {
		log.Warning(ctx, "upload: pod-restart %s has no in-memory state; timer_start_ms defaults to 0", key)
		return 0
	}
	return pr.TimerStartMs()
}

// Run polls Pass until the context ends, mirroring RunSealLoop: the §6.2
// upload worker plus the 03 §3.8-§3.9 recovery re-trigger (the first tick
// picks up whatever the previous process left pending).
func (u *Uploader) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := u.Pass(ctx); err != nil && ctx.Err() == nil {
				u.loopErrors.Add(1)
				log.Error(ctx, err, "upload pass failed")
			}
		}
	}
}

const dayMs = int64(24 * time.Hour / time.Millisecond)

func utcDayStartMs(ms int64) int64 { return ms - ms%dayMs }

func utcDayPath(ms int64) string { return time.UnixMilli(ms).UTC().Format("2006/01/02") }
