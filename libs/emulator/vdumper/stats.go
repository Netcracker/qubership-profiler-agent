package vdumper

import "time"

// StatsListener receives the virtual dumper's observable events. The feeder
// exposes them as logs and metrics; the k6 module maps them to samples.
// Callbacks run on the dumper goroutine — keep them cheap. Durations are
// measured on the configured Clock, so they read as zero under a fake clock.
type StatsListener interface {
	// Connected fires after a successful handshake and stream setup.
	Connected(incarnation int)
	// Disconnected fires when an incarnation dies; err is the cause.
	Disconnected(incarnation int, err error)
	// Churned fires when churn mode ends a healthy incarnation on purpose
	// (virtual-dumper.md §1.1); deliberate cycles never reach Disconnected
	// or AckError, so a storm run still sees real failures underneath.
	Churned(incarnation int)
	// StreamOpened fires per INIT_STREAM_V2, including rotations.
	StreamOpened(stream string, fileIndex int, reset bool)
	// BytesSent counts one RCV_DATA payload of the stream.
	BytesSent(stream string, n int)
	// AckError fires when the collector refused data with ACK_ERROR_MAGIC.
	AckError()
	// Dropped counts trace chunks lost while the dumper was down (the agent's
	// drop window: producers keep running, their output goes nowhere).
	Dropped(chunks int)
	// TcpConnected reports the TCP dial duration of one incarnation.
	TcpConnected(d time.Duration)
	// SessionReady reports the time from dial start to a usable session:
	// handshake answered and all seven streams opened. This is the
	// accept-latency signal for the T3 connection-ceiling runs.
	SessionReady(d time.Duration)
	// AckFlushed reports one stream's synchronous ack drain inside the
	// regular flush cycle. Rotation and params one-shot flushes are excluded,
	// so the series cleanly tracks ack-path degradation for the T2 runs.
	AckFlushed(stream string, d time.Duration)
}

// NoopStats discards every event.
type NoopStats struct{}

func (NoopStats) Connected(int)                    {}
func (NoopStats) Disconnected(int, error)          {}
func (NoopStats) Churned(int)                      {}
func (NoopStats) StreamOpened(string, int, bool)   {}
func (NoopStats) BytesSent(string, int)            {}
func (NoopStats) AckError()                        {}
func (NoopStats) Dropped(int)                      {}
func (NoopStats) TcpConnected(time.Duration)       {}
func (NoopStats) SessionReady(time.Duration)       {}
func (NoopStats) AckFlushed(string, time.Duration) {}
