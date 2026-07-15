// Package ingest routes the demultiplexed agent streams from the TCP server
// into the hot store: raw bytes into gzip segments, decoded records into the
// WALs and the SQLite call index (01-write-contract.md §4.3-§4.4). One
// Listener serves every connection; state is keyed per pod-restart.
package ingest

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/server"
	"github.com/pkg/errors"
)

type (
	// IngestStats is a snapshot of the ingest counters (№21). It replaces the
	// no-op metric stubs the Listener carried; RegisterIngest exposes each field
	// on the shared Prometheus registry Focus V set up.
	IngestStats struct {
		// CommandsReceived counts agent commands the server dispatched to the
		// listener; CommandErrors counts those that returned an error.
		CommandsReceived uint64
		CommandErrors    uint64
		// BytesRead is the total RCV_DATA payload bytes routed into streams.
		BytesRead uint64
		// DecoderErrors counts streams a decoder rejected as malformed or
		// gzip-wrapped (the agent then resends from scratch, 06 §6).
		DecoderErrors uint64
	}

	// Listener implements server.Listener on top of a hotstore.Store.
	Listener struct {
		store *hotstore.Store

		// Ingest counters (№21). Atomic so the /metrics scrape reads them
		// without taking the pod lock.
		commandsReceived atomic.Uint64
		commandErrors    atomic.Uint64
		bytesRead        atomic.Uint64
		decoderErrors    atomic.Uint64

		mu   sync.Mutex
		pods map[common.Uuid]*podIngest // keyed by the connection's pod UUID
	}

	// podIngest is the per-connection routing state: which stream file each
	// RCV_DATA handle feeds.
	podIngest struct {
		l  *Listener // for the shared ingest counters (№21)
		pr *hotstore.PodRestart

		mu       sync.Mutex
		byHandle map[common.Uuid]*fileIngest
		active   map[string]*fileIngest // current file per stream, for rotation
	}
)

// NewListener wires the TCP server's demux callbacks to the store.
func NewListener(store *hotstore.Store) *Listener {
	return &Listener{store: store, pods: map[common.Uuid]*podIngest{}}
}

func podKey(pod *server.ConnectedPod) hotstore.PodRestartKey {
	return hotstore.PodRestartKey{
		Namespace:     pod.Namespace,
		Service:       pod.Service,
		PodName:       pod.PodName,
		RestartTimeMs: pod.RestartTimeMs,
	}
}

// RegisterPod opens the pod-restart on the PV: directory layout plus the four
// WALs. Called once per connection, right after the handshake.
func (l *Listener) RegisterPod(pod *server.ConnectedPod) error {
	pr, err := l.store.OpenPodRestart(podKey(pod))
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pods[pod.Uuid] = &podIngest{
		l:        l,
		pr:       pr,
		byHandle: map[common.Uuid]*fileIngest{},
		active:   map[string]*fileIngest{},
	}
	return nil
}

func (l *Listener) pod(pod *server.ConnectedPod) (*podIngest, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	pi, ok := l.pods[pod.Uuid]
	if !ok {
		return nil, errors.Errorf("no registered pod-restart for connection %v", pod.Uuid)
	}
	return pi, nil
}

// RegisterStream starts ingesting one agent stream file. The agent opens the
// next rolling file with a fresh INIT_STREAM_V2, so a second registration of
// the same stream is a rotation: the previous file is finalized first.
func (l *Listener) RegisterStream(ctx context.Context,
	pod *server.ConnectedPod, handleId common.Uuid, streamType string,
	resetRequired int, requestedRollingSequenceId int, rollingSequenceId int,
	rotationPeriod uint64, requiredRotationSize uint64) error {

	pi, err := l.pod(pod)
	if err != nil {
		return err
	}

	pi.mu.Lock()
	defer pi.mu.Unlock()
	if prev, ok := pi.active[streamType]; ok {
		if err := pi.finalize(prev); err != nil {
			return errors.Wrapf(err, "finalize rotated %s file", streamType)
		}
		// The agent will not send RCV_DATA for the rotated file again; drop its
		// handle so byHandle does not grow one dead entry per rotation for the
		// life of the connection (wire-LOW).
		delete(pi.byHandle, prev.handle)
	}

	if streamType == model.StreamDictionary && resetRequired != 0 {
		pi.pr.ResetDictionary()
	}

	// The agent addresses this file as serverRollingSequenceId + 1 and puts
	// that value into every Call's trace_file_index; the segment must carry the
	// same name (01-write-contract.md §4.4).
	agentFileIndex := rollingSequenceId + 1
	fi, err := pi.openFile(ctx, streamType, agentFileIndex)
	if err != nil {
		return err
	}
	fi.handle = handleId
	pi.byHandle[handleId] = fi
	pi.active[streamType] = fi
	return nil
}

// AppendData routes one RCV_DATA payload into the handle's stream file. The
// payload arrives as a string because the framing layer reads it that way; the
// bytes pass through unmodified.
func (l *Listener) AppendData(ctx context.Context, pod *server.ConnectedPod, handleId common.Uuid, chunk string) (int, error) {
	if l.store.IngestPaused() {
		// №2 backpressure: refuse BEFORE writing anything, so the server
		// answers ACK_ERROR and the agent keeps the payload buffered in its
		// own files and retries after reconnecting.
		return 0, errors.Errorf("hot store over the pending-upload budget; refusing data of %v", pod.Uuid)
	}
	pi, err := l.pod(pod)
	if err != nil {
		return 0, err
	}
	pi.mu.Lock()
	fi, ok := pi.byHandle[handleId]
	pi.mu.Unlock()
	if !ok {
		return 0, errors.Errorf("no stream registered for handle %v", handleId)
	}
	if err := fi.write([]byte(chunk)); err != nil {
		return 0, err
	}
	l.bytesRead.Add(uint64(len(chunk)))
	return len(chunk), nil
}

// PodDisconnected finalizes the pod-restart: every stream file is closed and
// parsed to its end, the WALs get their footers, and the catalog row closes.
func (l *Listener) PodDisconnected(ctx context.Context, pod *server.ConnectedPod) {
	l.mu.Lock()
	pi, ok := l.pods[pod.Uuid]
	delete(l.pods, pod.Uuid)
	l.mu.Unlock()
	if !ok {
		return
	}
	pi.mu.Lock()
	defer pi.mu.Unlock()
	for _, fi := range pi.active {
		if err := pi.finalize(fi); err != nil {
			log.Error(ctx, err, "finalize %s on disconnect of %v", fi.stream, pi.pr.Key)
		}
	}
	pi.active = map[string]*fileIngest{}
	pi.byHandle = map[common.Uuid]*fileIngest{}
	if err := pi.pr.Close(); err != nil {
		log.Error(ctx, err, "close pod-restart %v", pi.pr.Key)
	}
}

// Close finalizes everything still open; used at collector shutdown.
func (l *Listener) Close(ctx context.Context) {
	l.mu.Lock()
	pods := make([]*podIngest, 0, len(l.pods))
	for _, pi := range l.pods {
		pods = append(pods, pi)
	}
	l.pods = map[common.Uuid]*podIngest{}
	l.mu.Unlock()
	for _, pi := range pods {
		pi.mu.Lock()
		for _, fi := range pi.active {
			if err := pi.finalize(fi); err != nil {
				log.Error(ctx, err, "finalize %s at shutdown", fi.stream)
			}
		}
		pi.active = map[string]*fileIngest{}
		pi.mu.Unlock()
		if err := pi.pr.Close(); err != nil {
			log.Error(ctx, err, "close pod-restart %v at shutdown", pi.pr.Key)
		}
	}
}

// IngestStatsSnapshot returns the ingest counters for the /metrics scrape (№21).
func (l *Listener) IngestStatsSnapshot() IngestStats {
	return IngestStats{
		CommandsReceived: l.commandsReceived.Load(),
		CommandErrors:    l.commandErrors.Load(),
		BytesRead:        l.bytesRead.Load(),
		DecoderErrors:    l.decoderErrors.Load(),
	}
}

// noteDecoderError records that a stream decoder rejected its input as malformed
// or gzip-wrapped (№21). The connection goroutine surfaces the same failure to
// the agent as ACK_ERROR_MAGIC via the pipe error.
func (l *Listener) noteDecoderError() { l.decoderErrors.Add(1) }

// Metric callbacks (№21): real counters registered by RegisterIngest.
func (l *Listener) SentCommand(ctx context.Context, c model.Command) {}
func (l *Listener) ReceivedCommand(ctx context.Context, c model.Command, latency time.Duration, err error) {
	l.commandsReceived.Add(1)
	if err != nil {
		l.commandErrors.Add(1)
	}
}
func (l *Listener) Read(ctx context.Context, bytes int, latency time.Duration, err error)  {}
func (l *Listener) Write(ctx context.Context, bytes int, latency time.Duration, err error) {}
func (l *Listener) IsAlive(ctx context.Context) (bool, error)                              { return true, nil }
func (l *Listener) Error(err error)                                                        {}
func (l *Listener) PrintDebug(ctx context.Context)                                         {}

var _ server.Listener = (*Listener)(nil)
