package hotstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
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

	// Uploader drains parquet_local to S3 and writes the per-pod-restart
	// snapshot objects (01-write-contract.md §3.6, §6.2; 03-lifecycle.md
	// §3.8-§3.9). One instance per collector process; Pass is single-threaded.
	Uploader struct {
		store *Store
		s3    ObjectStore

		mu       sync.Mutex
		counters UploadStats
	}

	// UploadStats counts one Pass's work (and, via CountersSnapshot, the
	// process lifetime — the seam for the future Prometheus counters).
	UploadStats struct {
		UploadedFiles    int64
		RetriedPuts      int64
		QuarantinedFiles int64
		ManifestPuts     int64
		SnapshotUploads  int64
		SegmentsDeleted  int64
	}

	// dictionarySnapshot is the S3 dictionary object (01 §3.6, 02 §2.6). The
	// wire dictionary is one id space shared by method and param references,
	// so both arrays carry the full word list: methods[method_id] and
	// params[param_id] resolve correctly under either reading of the contract.
	dictionarySnapshot struct {
		Version int      `json:"version"`
		Methods []string `json:"methods"`
		Params  []string `json:"params"`
	}

	suspendSnapshotEvent struct {
		StartMs    int64 `json:"start_ms"`
		DurationMs int   `json:"duration_ms"`
	}

	// suspendSnapshot is the per-pod-restart stop-the-world timeline (01 §3.6).
	suspendSnapshot struct {
		RestartTimeMs int64                  `json:"restart_time_ms"`
		TimerStartMs  int64                  `json:"timer_start_ms"`
		Events        []suspendSnapshotEvent `json:"events"`
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

func (u *Uploader) accumulate(stats UploadStats) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.counters.UploadedFiles += stats.UploadedFiles
	u.counters.RetriedPuts += stats.RetriedPuts
	u.counters.QuarantinedFiles += stats.QuarantinedFiles
	u.counters.ManifestPuts += stats.ManifestPuts
	u.counters.SnapshotUploads += stats.SnapshotUploads
	u.counters.SegmentsDeleted += stats.SegmentsDeleted
}

// Pass runs one upload round: pending parquet files, then the snapshot
// objects of closed pod-restarts, then the refcount-0 segment sweep. Per-file
// failures are logged and left for the next pass; only a context or SQLite
// failure aborts the pass.
func (u *Uploader) Pass(ctx context.Context) (UploadStats, error) {
	var stats UploadStats
	defer func() { u.accumulate(stats) }()

	if err := u.uploadPending(ctx, &stats); err != nil {
		return stats, err
	}
	if err := u.uploadSnapshots(ctx, &stats); err != nil {
		return stats, err
	}
	err := u.sweepSegments(ctx, &stats)
	return stats, err
}

// uploadPending drains parquet_local rows with uploaded_at IS NULL. Per file
// the order is fixed by invariant C1: PUT the object, refresh the day's pods
// manifest, and only then commit uploaded_at together with the refcount
// release — any failure before the commit leaves the row pending and the next
// pass redoes the idempotent PUTs.
func (u *Uploader) uploadPending(ctx context.Context, stats *UploadStats) error {
	files, err := u.store.db.PendingUploads()
	if err != nil {
		return err
	}
	manifestsDone := map[string]struct{}{}
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := os.Stat(f.Path); err != nil {
			// Recovery clears rows whose file is gone (03 §3.6); mid-flight the
			// pass just skips them.
			log.Warning(ctx, "upload: sealed parquet %s is missing on disk, skipping", f.Path)
			continue
		}
		err := u.putWithRetry(ctx, f.S3Key, func() error { return u.s3.PutFile(ctx, f.S3Key, f.Path) }, stats)
		if IsPermanentUploadError(err) {
			if qErr := u.quarantine(ctx, f, err, stats); qErr != nil {
				log.Error(ctx, qErr, "upload: quarantine of %s failed; the file stays pending", f.Path)
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return err
			}
			log.Error(ctx, err, "upload: PUT %s failed after retries; will retry next pass", f.S3Key)
			continue
		}
		if err := u.upsertManifest(ctx, f, manifestsDone, stats); err != nil {
			if ctx.Err() != nil {
				return err
			}
			// uploaded_at stays NULL, so the next pass re-runs the parquet PUT
			// and the manifest PUT; both are idempotent.
			log.Error(ctx, err, "upload: pods manifest for %s failed; %s stays pending", f.PodRestart, f.S3Key)
			continue
		}
		if err := u.store.db.MarkUploaded(f.Path, time.Now().UnixMilli()); err != nil {
			return err
		}
		stats.UploadedFiles++
		log.Info(ctx, "uploaded %s (%d rows)", f.S3Key, f.RowCount)
	}
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
	s3Key := path.Join("pods/v1", utcDayPath(dayStartMs), PodRestartHash(key)+".json")
	if err := u.putWithRetry(ctx, s3Key, func() error { return u.s3.PutBytes(ctx, s3Key, body) }, stats); err != nil {
		return err
	}
	done[doneKey] = struct{}{}
	stats.ManifestPuts++
	return nil
}

// uploadSnapshots writes the dictionary and suspend-timeline objects of every
// closed pod-restart still gated by dict_uploaded_at (01 §3.6, 03 §3.9). Both
// PUTs share the gate: a crash between them re-uploads both, idempotently.
func (u *Uploader) uploadSnapshots(ctx context.Context, stats *UploadStats) error {
	pending, err := u.store.db.DictPendingPodRestarts()
	if err != nil {
		return err
	}
	for _, podRestart := range pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		key, err := ParsePodRestartKey(podRestart)
		if err != nil {
			log.Error(ctx, err, "upload: skip snapshots of unparseable pod_restart %q", podRestart)
			continue
		}
		pr, ok := u.store.PodRestart(key)
		if !ok {
			log.Warning(ctx, "upload: pod-restart %s has no in-memory state; snapshots stay pending", podRestart)
			continue
		}
		if err := u.uploadPodSnapshots(ctx, pr, stats); err != nil {
			if ctx.Err() != nil {
				return err
			}
			log.Error(ctx, err, "upload: snapshots of %s failed; will retry next pass", podRestart)
		}
	}
	return nil
}

func (u *Uploader) uploadPodSnapshots(ctx context.Context, pr *PodRestart, stats *UploadStats) error {
	key := pr.Key
	// The snapshot keys derive their day from restart_time_ms: unlike the close
	// time, it survives a crash unchanged, keeping the key deterministic for
	// the recovery re-upload (§6.6) and for readers resolving a pod-restart.
	dayPath := utcDayPath(key.RestartTimeMs)
	hash := PodRestartHash(key)

	words := pr.DictionaryWords()
	dictBody, err := json.Marshal(dictionarySnapshot{Version: len(words), Methods: words, Params: words})
	if err != nil {
		return errors.Wrap(err, "encode dictionary snapshot")
	}
	// The shared helper keeps the writer and the cold reader on one key.
	dictKey := model.DictionarySnapshotKey(key.Tuple())
	if err := u.putWithRetry(ctx, dictKey, func() error { return u.s3.PutBytes(ctx, dictKey, dictBody) }, stats); err != nil {
		return err
	}

	pauses, err := readSuspendWal(pr)
	if err != nil {
		return err
	}
	events := make([]suspendSnapshotEvent, 0, len(pauses))
	for _, p := range pauses {
		events = append(events, suspendSnapshotEvent{StartMs: p.TimeMs, DurationMs: p.DurationMs})
	}
	suspendBody, err := json.Marshal(suspendSnapshot{
		RestartTimeMs: key.RestartTimeMs,
		TimerStartMs:  pr.TimerStartMs(),
		Events:        events,
	})
	if err != nil {
		return errors.Wrap(err, "encode suspend snapshot")
	}
	suspendKey := path.Join("suspend/v1", dayPath, hash+".json")
	if err := u.putWithRetry(ctx, suspendKey, func() error { return u.s3.PutBytes(ctx, suspendKey, suspendBody) }, stats); err != nil {
		return err
	}

	if err := u.store.db.SetDictUploaded(key.String(), time.Now().UnixMilli()); err != nil {
		return err
	}
	stats.SnapshotUploads++
	log.Info(ctx, "uploaded dictionary and suspend snapshots of %s (%d words, %d pauses)",
		key, len(words), len(events))
	return nil
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

// timerStartMs resolves the pod-restart's trace epoch for the manifest and
// suspend snapshots; 0 when the pod-restart never sent a trace stream.
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
				log.Error(ctx, err, "upload pass failed")
			}
		}
	}
}

const dayMs = int64(24 * time.Hour / time.Millisecond)

func utcDayStartMs(ms int64) int64 { return ms - ms%dayMs }

func utcDayPath(ms int64) string { return time.UnixMilli(ms).UTC().Format("2006/01/02") }
