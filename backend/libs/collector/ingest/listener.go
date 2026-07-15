// Package ingest routes the demultiplexed agent streams from the TCP server
// into the hot store: raw bytes into gzip segments, decoded records into the
// WALs and the SQLite call index (01-write-contract.md §4.3-§4.4). One
// Listener serves every connection; state is keyed per pod-restart.
package ingest

import (
	"context"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/server"
	"github.com/pkg/errors"
)

type (
	// Listener implements server.Listener on top of a hotstore.Store.
	Listener struct {
		store *hotstore.Store

		mu   sync.Mutex
		pods map[common.Uuid]*podIngest // keyed by the connection's pod UUID
	}

	// podIngest is the per-connection routing state: which stream file each
	// RCV_DATA handle feeds.
	podIngest struct {
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
	pi.byHandle[handleId] = fi
	pi.active[streamType] = fi
	return nil
}

// AppendData routes one RCV_DATA payload into the handle's stream file. The
// payload arrives as a string because the framing layer reads it that way; the
// bytes pass through unmodified.
func (l *Listener) AppendData(ctx context.Context, pod *server.ConnectedPod, handleId common.Uuid, chunk string) (int, error) {
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

// Metrics-oriented callbacks: nothing to observe yet (TODO seam: Prometheus
// counters land with the collector app wiring).
func (l *Listener) SentCommand(ctx context.Context, c model.Command) {}
func (l *Listener) ReceivedCommand(ctx context.Context, c model.Command, latency time.Duration, err error) {
}
func (l *Listener) Read(ctx context.Context, bytes int, latency time.Duration, err error)  {}
func (l *Listener) Write(ctx context.Context, bytes int, latency time.Duration, err error) {}
func (l *Listener) IsAlive(ctx context.Context) (bool, error)                              { return true, nil }
func (l *Listener) Error(err error)                                                        {}
func (l *Listener) PrintDebug(ctx context.Context)                                         {}

var _ server.Listener = (*Listener)(nil)
