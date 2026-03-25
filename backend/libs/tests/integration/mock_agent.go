package integration

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"time"
)

type MockAgentListener struct {
}

func CreateMockAgentListener() (m *MockAgentListener) {
	return &MockAgentListener{}
}

func (m *MockAgentListener) Command(c model.Command, latency time.Duration, err error) {
}

func (m *MockAgentListener) Read(bytes int, latency time.Duration, err error) {
}

func (m *MockAgentListener) Write(bytes int, latency time.Duration, err error) {
}

func (m *MockAgentListener) Error(err error) {
}

func (m *MockAgentListener) IsAlive() (bool, error) {
	return true, nil
}
