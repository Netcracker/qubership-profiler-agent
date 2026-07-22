package vdumper

import (
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
)

// ParamDef is one record of the params stream: the agent's parameter metadata
// (Dumper.prepareParamInfoStream; decoded by libs/parser/pipe/params.go).
type ParamDef struct {
	Name      string
	Index     bool
	List      bool
	Order     int
	Signature string
}

// Config parameterizes one emulated pod. Every load run records its full
// parameter set so runs stay comparable (load-testing-plan.md §4). The zero
// value is not runnable; New applies the defaults documented per field.
type Config struct {
	// Identity sent in the GET_PROTOCOL_VERSION_V2 handshake.
	Namespace, Service, PodName string

	// Connection carries the collector address and socket timeouts. The
	// read timeout defaults to the agent's PLAIN_SOCKET_READ_TIMEOUT (30 s).
	Connection emulator.ConnectionOpts

	// FlushInterval is the wall-clock cadence of the dumper flush cycle
	// (STREAM_FLUSH_INTERVAL, default 5 s).
	FlushInterval time.Duration
	// BufferStealInterval claims non-empty producer buffers that did not fill
	// (BUFFER_STEAL_INTERVAL, default 5 s).
	BufferStealInterval time.Duration
	// RestartInterval is the sleep between dumper incarnations after a failure
	// (DUMPER_RESTART_INTERVAL, default 10 s).
	RestartInterval time.Duration

	// ChurnInterval, when positive, disconnects every HEALTHY incarnation on
	// purpose after it lived this long past session-ready: an abrupt socket
	// close with no COMMAND_CLOSE — the CrashLoopBackOff shape of the T5
	// reconnect storms (virtual-dumper.md §1.1, churn mode). The pod then
	// follows the ordinary RestartInterval reconnect path. 0 disables churn.
	ChurnInterval time.Duration
	// ChurnJitter spreads the churn deadline uniformly by ± this fraction so
	// a fleet does not cycle in lockstep (default 0.2 when churn is on).
	ChurnJitter float64

	// DictionaryInitial is how many synthetic dictionary words the pod knows
	// at startup; the whole set is re-sent after every reconnect (default
	// 2000).
	DictionaryInitial int

	// ThreadsPerPod is the number of producer goroutines modeling application
	// threads. 0 keeps the pod idle — connection and keep-alive traffic only,
	// the T3 connection-ceiling shape.
	ThreadsPerPod int
	// CallsPerSecPerThread is the jittered root-call rate of one producer
	// (default 5).
	CallsPerSecPerThread float64
	// ChunkMaxBytes hands a producer buffer to the dumper once its encoded
	// events reach this size, mirroring a filled LocalBuffer (default 32 KB);
	// the buffer steal claims smaller ones after BufferStealInterval.
	ChunkMaxBytes int
	// ChunkQueueSize bounds the producer → dumper queue; a full queue drops
	// chunks (the reconnect drop window) instead of blocking producers
	// (default 64).
	ChunkQueueSize int
	// Seed makes the synthetic workload reproducible (default 1).
	Seed int64

	// Workload is the load-shape parameter set (§4 knobs); a zero value takes
	// DefaultWorkload().
	Workload Workload

	// Params is the params-stream payload, one-shot per connection; nil takes
	// a minimal default set.
	Params []ParamDef

	// Clock defaults to the wall clock; Stats defaults to a no-op listener.
	Clock Clock
	Stats StatsListener
}

func (c Config) withDefaults() Config {
	if c.FlushInterval == 0 {
		c.FlushInterval = 5 * time.Second
	}
	if c.BufferStealInterval == 0 {
		c.BufferStealInterval = 5 * time.Second
	}
	if c.RestartInterval == 0 {
		c.RestartInterval = 10 * time.Second
	}
	if c.ChurnInterval > 0 && c.ChurnJitter == 0 {
		c.ChurnJitter = 0.2
	}
	if c.DictionaryInitial == 0 {
		c.DictionaryInitial = 2000
	}
	if c.CallsPerSecPerThread == 0 {
		c.CallsPerSecPerThread = 5
	}
	if c.ChunkMaxBytes == 0 {
		c.ChunkMaxBytes = 32 * 1024
	}
	if c.ChunkQueueSize == 0 {
		c.ChunkQueueSize = 64
	}
	if c.Seed == 0 {
		c.Seed = 1
	}
	if c.Params == nil {
		c.Params = []ParamDef{
			{Name: "request.id", Index: true},
			{Name: "call.red", Index: true},
			{Name: "sql"},
			{Name: "xml"},
		}
	}
	if c.Workload.isZero() {
		c.Workload = DefaultWorkload()
	}
	if c.Connection.Timeout.ConnectTimeout == 0 {
		c.Connection.Timeout.ConnectTimeout = 10 * time.Second
	}
	if c.Connection.Timeout.SessionTimeout == 0 {
		c.Connection.Timeout.SessionTimeout = 24 * time.Hour
	}
	if c.Connection.Timeout.ReadTimeout == 0 {
		c.Connection.Timeout.ReadTimeout = 30 * time.Second
	}
	if c.Connection.Timeout.WriteTimeout == 0 {
		c.Connection.Timeout.WriteTimeout = 5 * time.Second
	}
	if c.Clock == nil {
		c.Clock = RealClock()
	}
	if c.Stats == nil {
		c.Stats = NoopStats{}
	}
	return c
}
