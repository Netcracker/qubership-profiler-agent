package emulator

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"time"
)

type (
	// AgentListener (overridden in cdt-loader-generator)
	AgentListener interface {
		Command(c model.Command, latency time.Duration, err error)
		Read(bytes int, latency time.Duration, err error)
		Write(bytes int, latency time.Duration, err error)
		Error(err error)
		IsAlive() (bool, error)
	}
)
