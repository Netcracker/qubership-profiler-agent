package hotstore

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/parser/pipe"
	"github.com/pkg/errors"
)

// Recover rebuilds the store's state from the PV per 03-lifecycle.md §3
// (steps 3.3-3.5, plus the calls.wal reconciliation of step 3.4). Every
// pod-restart found on disk is treated as closed: the crash broke its TCP
// connection and the agent has reconnected elsewhere as a fresh pod-restart.
// Sealing and upload retries (steps 3.6-3.9) land with the seal pass.
//
// 03 §4 is "degrade, not fail" (№26): a pod-restart whose on-disk state does
// not recover is quarantined under recovery-failed/ instead of crash-looping
// the whole collector, and the pod-restarts rescan in parallel so a PV full
// of gzip segments does not serialize the port opening on one core.
func (s *Store) Recover(ctx context.Context) error {
	if err := s.db.CloseAllOpen(time.Now().UnixMilli()); err != nil {
		return errors.Wrap(err, "close open pod-restarts")
	}

	keys, err := listPodRestartDirs(s.cfg.DataDir)
	if err != nil {
		return err
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > 8 {
		workers = 8 // gunzip-bound; more workers only contend on SQLite
	}
	if workers > len(keys) {
		workers = len(keys)
	}
	var (
		wg       sync.WaitGroup
		jobs     = make(chan PodRestartKey)
		fatalMu  sync.Mutex
		fatalErr error
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				if ctx.Err() != nil {
					continue
				}
				pr, err := s.recoverPodRestart(ctx, key)
				if err != nil {
					if qErr := s.quarantinePodRestart(ctx, key, err); qErr != nil {
						fatalMu.Lock()
						if fatalErr == nil {
							fatalErr = qErr
						}
						fatalMu.Unlock()
					}
					continue
				}
				s.mu.Lock()
				s.pods[key.String()] = pr
				s.mu.Unlock()
			}
		}()
	}
	for _, key := range keys {
		jobs <- key
	}
	close(jobs)
	wg.Wait()
	if fatalErr != nil {
		return fatalErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.reconcileParquetLocal(ctx); err != nil {
		return err
	}
	// The backpressure gates must reflect the backlog a previous process left
	// behind before the TCP listener opens (№2).
	return s.refreshBackpressure(ctx)
}

// quarantinePodRestart implements the №26 degrade path: the pod-restart's
// directory moves under recovery-failed/ for a human, its index rows leave
// the partitions (they would poison every seal and pin the contiguity
// barrier), and its snapshot uploads quarantine like a permanent S3 rejection
// so no loop waits on state that is gone. Only a filesystem/SQLite failure of
// the quarantine itself is returned — that one still fails recovery.
func (s *Store) quarantinePodRestart(ctx context.Context, key PodRestartKey, cause error) error {
	log.Error(ctx, cause, "recovery: pod-restart %s does not recover; quarantining its directory", key)
	dir := key.dir(s.cfg.DataDir)
	dest := filepath.Join(s.cfg.DataDir, "recovery-failed",
		fmt.Sprintf("%s_%s_%s_%d", key.Namespace, key.Service, key.PodName, key.RestartTimeMs))
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return errors.Wrap(err, "create recovery-failed dir")
	}
	if err := os.RemoveAll(dest); err != nil { // a previous quarantine of the same key
		return errors.Wrap(err, "clear stale quarantine")
	}
	if err := os.Rename(dir, dest); err != nil {
		return errors.Wrap(err, "quarantine pod-restart dir")
	}
	removeEmptyParents(filepath.Join(s.cfg.DataDir, "pods"), filepath.Dir(dir))
	purged, err := s.db.PurgeCallsPastWalEnd(key.String(), 0)
	if err != nil {
		return err
	}
	if err := s.db.SetDictUploadFailed(key.String(), time.Now().UnixMilli()); err != nil {
		return err
	}
	log.Warning(ctx, "recovery: quarantined %s under %s (%d index rows dropped); its calls are lost to the hot tier",
		key, dest, purged)
	return nil
}

// reconcileParquetLocal implements 03-lifecycle.md §3.6 step 10 (second half):
// a parquet_local row whose file is missing on disk is cleared, releasing the
// segment refs it pinned, so the bucket re-seals its rows; a sealed FILE with
// no catalog row — a crash between the №6 rename and the pass commit — is
// removed, because its rows re-seal from the watermark and normally reproduce
// the same name, but a dictionary grown between the passes can shift a row's
// class and leave the old name orphaned forever. (Rebuilding parquet_local
// from orphan files' footers — the §3.2 step-4 repair — is not implemented
// yet; see the Stage 1 open issues.)
func (s *Store) reconcileParquetLocal(ctx context.Context) error {
	paths, err := s.db.ParquetLocalPaths()
	if err != nil {
		return err
	}
	catalogued := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		catalogued[path] = struct{}{}
		if _, err := os.Stat(path); err == nil {
			continue
		}
		log.Warning(ctx, "sealed parquet %s is missing on disk; clearing its catalog row", path)
		if err := s.db.DropParquetLocal(path); err != nil {
			return err
		}
	}
	sealedRoot := filepath.Join(s.cfg.DataDir, "parquet")
	err = filepath.WalkDir(sealedRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".parquet") {
			return nil //nolint:nilerr // a missing root means nothing sealed yet
		}
		if _, ok := catalogued[path]; ok {
			return nil
		}
		log.Warning(ctx, "sealed parquet %s has no catalog row (crash before the seal commit); removing the orphan", path)
		return os.Remove(path)
	})
	if err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "sweep orphan sealed parquet")
	}
	return nil
}

// listPodRestartDirs walks /data/pods/<ns>/<svc>/<pod>/<restartMs>. The PV
// layout, not the SQLite catalog, is the recovery source of truth.
func listPodRestartDirs(dataDir string) ([]PodRestartKey, error) {
	var keys []PodRestartKey
	root := filepath.Join(dataDir, "pods")
	level := func(dir string) ([]string, error) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, errors.Wrapf(err, "read %s", dir)
		}
		var names []string
		for _, e := range entries {
			if e.IsDir() {
				names = append(names, e.Name())
			}
		}
		return names, nil
	}
	namespaces, err := level(root)
	if err != nil {
		return nil, err
	}
	for _, ns := range namespaces {
		services, err := level(filepath.Join(root, ns))
		if err != nil {
			return nil, err
		}
		for _, svc := range services {
			pods, err := level(filepath.Join(root, ns, svc))
			if err != nil {
				return nil, err
			}
			for _, pod := range pods {
				restarts, err := level(filepath.Join(root, ns, svc, pod))
				if err != nil {
					return nil, err
				}
				for _, restart := range restarts {
					ms, err := strconv.ParseInt(restart, 10, 64)
					if err != nil {
						continue // not a restart dir; leave it alone
					}
					keys = append(keys, PodRestartKey{Namespace: ns, Service: svc, PodName: pod, RestartTimeMs: ms})
				}
			}
		}
	}
	return keys, nil
}

func (s *Store) recoverPodRestart(ctx context.Context, key PodRestartKey) (*PodRestart, error) {
	pr := &PodRestart{
		Key:       key,
		store:     s,
		dir:       key.dir(s.cfg.DataDir),
		closed:    true, // recovered pod-restarts never accept new writes
		finalized: true,
		dict:      map[int]string{},
		dictIds:   map[string]int{},
		chunks:    map[uint64][]ChunkRef{},
		segments:  map[*Segment]struct{}{},
	}

	// A crash can precede the SQLite insert; the closed row must still exist.
	if err := s.db.UpsertPodRestart(key, key.RestartTimeMs); err != nil {
		return nil, err
	}
	if err := s.db.ClosePodRestart(key, time.Now().UnixMilli()); err != nil {
		return nil, err
	}

	// A seal pass in flight at crash time left footer-less scratch files (and
	// blob spill files); discard them — the bucket re-seals from its watermark
	// (03-lifecycle.md §3.6 step 10).
	if err := os.RemoveAll(filepath.Join(pr.dir, "parquet-sealing")); err != nil {
		return nil, errors.Wrap(err, "discard seal scratch")
	}

	if err := pr.replayDictionary(); err != nil {
		return nil, err
	}
	// params.wal is validated (and its torn tail truncated) even though this
	// slice keeps nothing from it in RAM.
	if _, err := replayIfPresent(filepath.Join(pr.dir, "params.wal"), func(int64, []byte) error { return nil }); err != nil {
		return nil, err
	}
	// suspend.wal is replayed into the RAM pause mirror BEFORE reconcileCalls,
	// so a re-indexed call gets the same index-time suspend_ms attribution as
	// the original insert. The seal pass still reads suspend.wal by offset 0.
	pauses, err := readSuspendWal(pr)
	if err != nil {
		return nil, err
	}
	pr.pauses = pauses

	if err := pr.rescanSegments(ctx); err != nil {
		return nil, err
	}
	if err := pr.reconcileCalls(); err != nil {
		return nil, err
	}
	// Recovered pod-restarts are all closed; the dictionary served the
	// reconciliation above and reloads lazily from now on (№1), so recovering
	// a PV full of pod-restarts does not front-load their words into RAM.
	pr.unloadDictionary()
	return pr, nil
}

// replayIfPresent replays a WAL, treating a missing file as empty: a crash
// between TCP accept and the first record leaves no WAL (03 §3.4). clean
// mirrors ReplayWal: a verified footer was found.
func replayIfPresent(path string, apply func(offset int64, body []byte) error) (clean bool, err error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false, nil
	}
	return ReplayWal(path, apply)
}

// replayDictionary rebuilds the in-RAM dictionary from dictionary.wal
// (01 §3.2: varint(word_id) varint(word_len) word_bytes).
func (pr *PodRestart) replayDictionary() error {
	_, err := replayIfPresent(filepath.Join(pr.dir, "dictionary.wal"), func(_ int64, body []byte) error {
		return pr.applyDictRecordLocked(body)
	})
	return err
}

// rescanSegments implements 03 §3.5: walk the hot-store segments, rebuild the
// catalog rows, and re-parse trace chunks into chunk_index[threadId].
func (pr *PodRestart) rescanSegments(ctx context.Context) error {
	for _, stream := range []string{StreamTrace, StreamSql, StreamXml} {
		dir := filepath.Join(pr.dir, stream)
		entries, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return errors.Wrapf(err, "read %s", dir)
		}
		var seqs []int
		for _, e := range entries {
			name := e.Name()
			if !strings.HasSuffix(name, ".gz") {
				continue
			}
			seq, err := strconv.Atoi(strings.TrimSuffix(name, ".gz"))
			if err != nil {
				continue
			}
			seqs = append(seqs, seq)
		}
		sort.Ints(seqs) // chunk_index must keep the agent's write order
		for _, seq := range seqs {
			if err := pr.rescanSegment(ctx, stream, seq); err != nil {
				return err
			}
		}
	}
	return nil
}

func (pr *PodRestart) rescanSegment(ctx context.Context, stream string, seq int) error {
	path := filepath.Join(pr.dir, stream, SegmentFileName(seq))
	if err := pr.store.db.UpsertSegment(pr.Key.String(), stream, seq, path, time.Now().UnixMilli()); err != nil {
		return err
	}

	f, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "open segment")
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		// The whole segment is torn (crash inside the first gzip block);
		// catalog it as empty rather than failing recovery.
		log.Warning(ctx, "segment %s is unreadable, cataloguing as empty: %v", path, err)
		return pr.store.db.FinalizeSegment(pr.Key.String(), stream, seq, 0, nil, nil)
	}

	if stream != StreamTrace {
		// sql/xml carry offset-addressed values, not chunked events: record the
		// decompressed length without parsing the body (03 §3.5 step 9).
		size := tolerantCount(gz)
		return pr.store.db.FinalizeSegment(pr.Key.String(), stream, seq, size, nil, nil)
	}

	var timeMin, timeMax *int64

	// Every trace file opens with the same 8-byte timer epoch; re-read it here
	// (01 §4.3) and hand the parser the stream it expects.
	var header [8]byte
	if _, err := io.ReadFull(gz, header[:]); err != nil {
		log.Warning(ctx, "segment %s has no complete epoch header: %v", path, err)
		return pr.store.db.FinalizeSegment(pr.Key.String(), stream, seq, 0, nil, nil)
	}
	pr.SetTimerStart(int64(binary.BigEndian.Uint64(header[:])))

	// Count the decompressed bytes actually delivered: the parser's own
	// position overcounts at EOF, where a failed fixed-width read still
	// advances it.
	counter := &countingReader{r: tolerantReader{gz}}
	reader := pipe.NewPipeReader(io.MultiReader(bytes.NewReader(header[:]), counter), false)
	for item := range pipe.TracesPipeReader(ctx, reader) {
		if !item.Complete {
			// A torn tail chunk is not indexed; its bytes stay in the segment.
			// Keep draining so the parser goroutine can finish.
			continue
		}
		startMs := item.Time.UnixMilli()
		pr.chunks[item.ThreadId] = append(pr.chunks[item.ThreadId], ChunkRef{
			RollingSeq: seq, Offset: item.Offset, Length: item.Size(), StartMs: startMs,
		})
		if timeMin == nil || startMs < *timeMin {
			v := startMs
			timeMin = &v
		}
		if timeMax == nil || startMs > *timeMax {
			v := startMs
			timeMax = &v
		}
	}
	// The parser has exited (channel closed), so the count is complete: the
	// 8-byte epoch plus every decompressed byte, torn tail included.
	return pr.store.db.FinalizeSegment(pr.Key.String(), stream, seq, int64(len(header))+counter.n, timeMin, timeMax)
}

// reconcileCalls implements the 03 §3.4 step-7 reconciliation: any calls.wal
// record past the highest indexed calls_wal_offset (a crash hit between the
// WAL append and the SQLite insert) is re-indexed into its bucket's partition.
// The inverse skew is repaired too (№8): after a power loss the index can run
// AHEAD of a truncated calls.wal (SQLite synced, the WAL tail did not), and
// such rows would poison every seal of their bucket — loadSealRows can never
// find their records. They are dropped together with the torn tail.
func (pr *PodRestart) reconcileCalls() error {
	maxOffset, indexed, err := pr.store.db.MaxCallsWalOffset(pr.Key.String())
	if err != nil {
		return err
	}
	walPath := filepath.Join(pr.dir, "calls.wal")
	clean, err := replayIfPresent(walPath, func(offset int64, body []byte) error {
		if indexed && offset <= maxOffset {
			return nil
		}
		var rec CallWalRecord
		if err := json.Unmarshal(body, &rec); err != nil {
			return errors.Wrap(err, "decode calls.wal record")
		}
		return pr.indexCall(rec.TsMs, rec.Call, offset)
	})
	if err != nil {
		return err
	}
	// ReplayWal already truncated any torn tail, so the file ends at the last
	// valid record (or at the footer on a clean close): everything indexed at
	// or past that end has no record left to seal from.
	validEnd := int64(0)
	if info, err := os.Stat(walPath); err == nil {
		validEnd = info.Size()
		if clean {
			validEnd -= walFooterSize
		}
	}
	purged, err := pr.store.db.PurgeCallsPastWalEnd(pr.Key.String(), validEnd)
	if err != nil {
		return err
	}
	if purged > 0 {
		log.Warning(context.Background(), "recovery: dropped %d index rows of %s pointing past the end of calls.wal; their records were lost with the torn tail (№8)",
			purged, pr.Key)
	}
	return nil
}

// countingReader counts the bytes its inner reader delivered.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// tolerantReader converts a torn gzip tail (io.ErrUnexpectedEOF) into a plain
// EOF: recovery keeps everything before the tear.
type tolerantReader struct{ r io.Reader }

func (t tolerantReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return n, io.EOF
	}
	return n, err
}

// tolerantCount counts decompressed bytes up to EOF or a torn tail.
func tolerantCount(r io.Reader) int64 {
	n, _ := io.Copy(io.Discard, tolerantReader{r})
	return n
}
