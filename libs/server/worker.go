package server

import (
	"time"

	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
)

type (
	// Worker (should be overridden in collector)
	Worker interface {
		Command(c model.Command, latency time.Duration, err error)
		Read(bytes int, latency time.Duration, err error)
		Write(bytes int, latency time.Duration, err error)
		Error(err error)
		IsAlive() (bool, error)
	}
)
