package integration

import (
	"bytes"
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/server"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
)

type (
	MockServerListener struct {
		mu                sync.Mutex
		pods              map[string]*MockPod
		inCommands        map[model.Command]int
		inBytes, outBytes uint64
	}

	MockPod struct {
		*server.ConnectedPod
		streams map[string]*MockStream
	}
	MockStream struct {
		uuid common.Uuid
		data bytes.Buffer
	}
	MockCommand struct {
		namespace, service, pod string
	}
)

func CreateMockServerListener() *MockServerListener {
	return &MockServerListener{
		pods:       map[string]*MockPod{},
		inCommands: map[model.Command]int{},
	}
}

func (m *MockServerListener) RegisterPod(pod *server.ConnectedPod) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pods[pod.PodName] = &MockPod{ConnectedPod: pod, streams: make(map[string]*MockStream)}
	return nil
}

func (m *MockServerListener) AppendData(ctx context.Context, pod *server.ConnectedPod, handleId common.Uuid, chunk string) (int, error) {
	return len(chunk), nil
}

func (m *MockServerListener) RegisterStream(ctx context.Context, pod *server.ConnectedPod,
	handleId common.Uuid, streamType string, resetRequired int, requestedRollingSequenceId int,
	rollingSequenceId int, rotationPeriod uint64, requiredRotationSize uint64) error {

	m.mu.Lock()
	defer m.mu.Unlock()
	m.pods[pod.PodName] = &MockPod{ConnectedPod: pod, streams: make(map[string]*MockStream)}
	return nil
}

func (m *MockServerListener) PodDisconnected(ctx context.Context, pod *server.ConnectedPod) {
}

func (m *MockServerListener) ReceivedCommand(ctx context.Context, c model.Command, latency time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, has := m.inCommands[c]; !has {
		m.inCommands[c] = 0
	}
	m.inCommands[c]++
}

func (m *MockServerListener) Read(ctx context.Context, bytes int, latency time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inBytes += uint64(bytes)
}

func (m *MockServerListener) Write(ctx context.Context, bytes int, latency time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outBytes += uint64(bytes)
}

func (m *MockServerListener) PrintDebug(ctx context.Context) {
}

func (m *MockServerListener) Close(ctx context.Context) {
}
