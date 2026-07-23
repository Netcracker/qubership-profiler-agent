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
		FailedPuts       int64
		RetriedPuts      int64
		QuarantinedFiles int64
		// QuarantinedObjects counts manifest/snapshot quarantine ATTEMPTS, not
		// distinct objects: the slow re-test re-attempts a stuck manifest every
		// interval, and each fresh rejection re-quarantines (overwrites) the same
		// upload-failed/ body and bumps this counter. Read its rate as "how often
		// a permanently rejected object is re-tested", not "how many objects are
		// stuck" — QuarantineStats (first_failed_at) is the stuck-population gauge.
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
		manifestsDone = map[string]manifestOutcome{}
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
	manifestMu *sync.Mutex, manifestsDone map[string]manifestOutcome) error {

	// A re-queued manifest-quarantined row already has its body in S3
	// (body_durable_at set): skip the body PUT and re-test only the pending
	// manifest, so a stuck manifest never re-PUTs the whole day's parquet bodies
	// on every re-test interval (#825). The body PUT is idempotent, so a crash
	// between the PUT and MarkBodyDurable simply re-PUTs once next pass.
	if f.BodyDurableAtMs == nil {
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
		// The body is durable now. Record it before the coupled manifest so any
		// manifest failure below re-tests only the manifest, never the body.
		if err := u.store.db.MarkBodyDurable(f.Path, time.Now().UnixMilli()); err != nil {
			return err
		}
	}
	// The manifest upsert is serialized across workers: manifestsDone dedups
	// per (pod-restart, day) and the PUT itself is idempotent, so coarse
	// serialization is correct and manifests are rare next to parquet PUTs.
	manifestMu.Lock()
	outcome, mErr := u.upsertManifest(ctx, f, manifestsDone, stats)
	manifestMu.Unlock()
	if mErr != nil {
		if ctx.Err() != nil {
			return mErr
		}
		// uploaded_at stays NULL, so the next pass re-runs the manifest PUT; the
		// body is already durable (body_durable_at set above), so it is not
		// re-PUT. Both are idempotent.
		log.Error(ctx, mErr, "upload: pods manifest for %s failed; %s stays pending", f.PodRestart, f.S3Key)
		return nil
	}
	if outcome == manifestRejected {
		// The manifest is the only durable, readable source of the pod-restart's
		// (namespace, service, pod) identity behind the one-way hash in the
		// parquet key (01 §3.6, 02 §2.7). Marking the parquet uploaded here would
		// let the hot tier expire and the WALs purge, leaving a cold call that
		// discovery finds by time range but can never name — pod identity lost.
		// Quarantine the parquet WITHOUT marking it uploaded instead: it leaves
		// the tight upload queue, keeps the pod-restart discoverable/pending, and
		// the №2 slow re-test (requeueQuarantined) retries the manifest on the
		// same rate-limited cadence until S3 takes it.
		if qErr := u.store.db.QuarantineForManifest(f.Path, time.Now().UnixMilli()); qErr != nil {
			return qErr
		}
		stats.QuarantinedFiles++
		log.Warning(ctx, "upload: quarantined %s until its pod manifest is durable; the pod-restart stays pending, not silently undiscoverable", f.S3Key)
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
// PV data layout, the parquet_local row (and its parquet_segments refs) follow
// it and leave the upload queue, and — critically — those segment refs survive
// so the refcounts stay pinned until a human resolves the rejection.
//
// The destination mirrors the full S3 key under upload-failed/ (like
// quarantineObject does for manifests), not just filepath.Base(f.Path): the
// sealed basename omits the retention class — it lives only in the S3-key
// directory — so two files of different classes can share a basename and would
// overwrite each other in a single flat directory (QA 708#3). The S3 key is
// unique per file, so this destination never collides, and it stays under
// upload-failed/ for the capQuarantine drop-log heuristic and the №2 re-test.
//
// When the №2 re-test heals and the re-upload succeeds, MarkUploaded stamps
// uploaded_at on the row at this quarantine path but deliberately leaves the
// file in place — there is no parquet analogue to clearQuarantinedObject. The
// local parquet may still back hot reads, and hot-retention reaps it later by
// parquet_local.path (§6.3), exactly as it does for a normally uploaded file;
// only the small manifest snapshot, which backs nothing, is cleared eagerly.
// Do NOT make MarkUploaded delete the file: it also runs on the normal,
// non-quarantine upload path, where the live parquet must survive until
// retention.
func (u *Uploader) quarantine(ctx context.Context, f ParquetLocalFile, cause error, stats *UploadStats) error {
	dest := filepath.Join(u.store.cfg.DataDir, "upload-failed", filepath.FromSlash(f.S3Key))
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return errors.Wrap(err, "create upload-failed dir")
	}
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

// manifestOutcome records, per (pod-restart, UTC day) within one upload pass,
// whether the pods manifest is durable in S3 (manifestDurable) or was
// permanently rejected and quarantined this pass (manifestRejected). It couples
// the parquet's uploaded_at commit to the manifest across every sibling file of
// the same pod-restart and day: the manifest is the only cold source of the
// pod-restart's readable identity (01 §3.6, 02 §2.7), so a rejected manifest
// must keep every sibling out of "uploaded", not just the file that hit the
// rejection.
type manifestOutcome int

const (
	manifestDurable manifestOutcome = iota
	manifestRejected
)

// upsertManifest writes or refreshes the pods manifest of the file's UTC day
// (01 §3.6): idempotent per (day, pod-restart), bounds over every file the
// pod-restart sealed into that day so a later seal only widens time_max_ms.
// It returns manifestDurable once the manifest is confirmed in S3 and
// manifestRejected when S3 permanently refuses it: the caller then quarantines
// the parquet instead of marking it uploaded, so the pod-restart is never left
// discoverable-but-nameless. A transient failure returns an error and the file
// stays pending for the next pass. The permanent rejection is NOT terminal —
// the manifest body is kept under upload-failed/ for inspection, but the №2
// slow re-test retries it, so a healed (operational) rejection recovers.
func (u *Uploader) upsertManifest(ctx context.Context, f ParquetLocalFile, done map[string]manifestOutcome, stats *UploadStats) (manifestOutcome, error) {
	dayStartMs := utcDayStartMs(f.TimeBucketMs)
	doneKey := fmt.Sprintf("%s/%d", f.PodRestart, dayStartMs)
	if outcome, ok := done[doneKey]; ok {
		return outcome, nil
	}
	key, err := ParsePodRestartKey(f.PodRestart)
	if err != nil {
		return manifestRejected, err
	}
	s3Key := path.Join("pods/v1", utcDayPath(dayStartMs), PodRestartHash(key)+".json")
	timeMinMs, timeMaxMs, ok, err := u.store.db.ManifestBounds(f.PodRestart, dayStartMs, dayStartMs+dayMs)
	if err != nil {
		return manifestRejected, err
	}
	if !ok {
		return manifestRejected, errors.Errorf("no sealed files for %s on day %d despite pending upload", f.PodRestart, dayStartMs)
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
		return manifestRejected, errors.Wrap(err, "encode pods manifest")
	}
	if err := u.putWithRetry(ctx, s3Key, func() error { return u.s3.PutBytes(ctx, s3Key, body) }, stats); err != nil {
		if !IsPermanentUploadError(err) {
			return manifestRejected, err
		}
		if qErr := u.quarantineObject(ctx, s3Key, body, err); qErr != nil {
			return manifestRejected, qErr
		}
		// Counts a quarantine ATTEMPT: the slow re-test lands here again on every
		// interval a stuck manifest stays rejected (see the field doc).
		stats.QuarantinedObjects++
		done[doneKey] = manifestRejected
		return manifestRejected, nil
	}
	// The manifest is durable now; drop any stale rejected copy so upload-failed/
	// reflects only what still needs a human.
	u.clearQuarantinedObject(ctx, s3Key)
	done[doneKey] = manifestDurable
	stats.ManifestPuts++
	return manifestDurable, nil
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

// clearQuarantinedObject removes a previously quarantined object body once its
// key uploads cleanly, so upload-failed/ never keeps a stale copy of a manifest
// (or snapshot) that is now durable. Best-effort: a missing file is the common
// case and any other error is logged, not fatal.
func (u *Uploader) clearQuarantinedObject(ctx context.Context, s3Key string) {
	dest := filepath.Join(u.store.cfg.DataDir, "upload-failed", filepath.FromSlash(s3Key))
	if err := os.Remove(dest); err != nil && !os.IsNotExist(err) {
		log.Warning(ctx, "upload: cannot clear stale quarantined object %s: %v", dest, err)
	}
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
