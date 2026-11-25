// Package tcp carries profiler agent connections to the collector, or
// terminates and discards them. It has no Collector imports.
//
// Every connection opens with the agent's handshake, which names its
// namespace, service, and pod. The proxy reads it, picks the mode from the
// rules, and then either replays it to the upstream and copies bytes between
// the two sockets in both directions, or hands the connection to a libs/wire
// server that runs the protocol and stores nothing.
//
// The agent's own retry shapes the design. A connection it cannot
// establish, or one that drops, makes the agent mark its dumper dead, log
// one warning, drop its buffers, and retry after DUMPER_RESTART_INTERVAL,
// 10 seconds by default (com.netcracker.profiler.Dumper.RESTART_INTERVAL).
// So the proxy resets the agent socket on every end the agent did not start,
// except in an established discard session: the connection cap, a bad
// handshake, an unreachable upstream, the upstream ending a passthrough
// session, a rules change, and shutdown. The agent recovers from a reset on
// its own.
//
// An established discard session that ends on its own ends the way a
// session with the collector does: the libs/wire server closes the socket
// with a FIN after its read timeout, a protocol error, or the agent's
// COMMAND_CLOSE.
package tcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	wirelog "github.com/Netcracker/qubership-profiler-agent/libs/wire/log"
	"github.com/Netcracker/qubership-profiler-agent/libs/wire/server"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
)

// finalAnswerTimeout bounds the wait for the upstream to close its side after
// the agent finished sending. It matches the agent's own 30-second read
// timeout.
const finalAnswerTimeout = 30 * time.Second

// Config is the validated configuration of a Proxy.
type Config struct {
	// Endpoint is the listen address, host:port.
	Endpoint string
	// Upstream is the collector's host:port. Empty is allowed only when
	// every decision is discard.
	Upstream       string
	ConnectTimeout time.Duration
	MaxConnections int
	// CopyBufferSize is the copy buffer per direction.
	CopyBufferSize int
	// HandshakeTimeout bounds the wait for the agent's handshake.
	HandshakeTimeout time.Duration
	// RotationPeriod and RequiredRotationSize are the INIT_STREAM_V2 reply
	// fields a discarding connection sends the agent.
	RotationPeriod       time.Duration
	RequiredRotationSize uint64
}

// Proxy is the agent connection listener.
type Proxy struct {
	cfg     Config
	policy  *rules.Policy
	logger  *zap.Logger
	metrics *metrics
	sem     chan struct{}
	bufs    sync.Pool
	// sink runs the collector protocol for discarded connections.
	sink *server.Service
	// sinkCtx routes the libs/wire log records of discarded connections
	// to logger instead of stdout.
	sinkCtx context.Context

	ln     net.Listener
	mu     sync.Mutex
	conns  map[*session]struct{}
	closed bool
	wg     sync.WaitGroup
	// finalAnswer is finalAnswerTimeout; tests shorten it.
	finalAnswer time.Duration
}

// session is one agent connection and, in passthrough, its upstream leg.
type session struct {
	client net.Conn
	mu     sync.Mutex
	// upstream, mode, attrs, decided, and closed are guarded by mu. decided
	// turns true once the mode is chosen; Reevaluate skips sessions still
	// reading their handshake, which will see the new rules when they decide.
	upstream net.Conn
	mode     rules.Mode
	attrs    rules.Attrs
	decided  bool
	closed   bool
}

// close closes both legs. It is safe to call more than once.
func (s *session) close() {
	_ = s.client.Close()
	s.mu.Lock()
	s.closed = true
	up := s.upstream
	s.mu.Unlock()
	if up != nil {
		_ = up.Close()
	}
}

// setUpstream records up as the upstream leg. When the session was closed
// during the dial, by a rules change or shutdown, it closes up and returns
// false, so the handshake is never replayed to the upstream.
func (s *session) setUpstream(up net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = up.Close()
		return false
	}
	s.upstream = up
	return true
}

// abort closes the agent leg with a reset and the upstream leg normally.
func (s *session) abort() {
	abort(s.client)
	s.close()
}

// abort closes c with a TCP RST instead of a FIN. The agent treats EOF as
// "no data yet" (EndlessSocketInputStream) and spins until its 30 s read
// timeout; a reset fails its read at once, so it retries after its restart
// interval.
func abort(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = c.Close()
}

// New builds a Proxy. It opens no socket.
func New(cfg Config, policy *rules.Policy, logger *zap.Logger, meter metric.Meter) (*Proxy, error) {
	m, err := newMetrics(meter)
	if err != nil {
		return nil, fmt.Errorf("create agent connection metrics: %w", err)
	}
	p := &Proxy{
		cfg:     cfg,
		policy:  policy,
		logger:  logger,
		metrics: m,
		sem:     make(chan struct{}, cfg.MaxConnections),
		conns:   map[*session]struct{}{},

		finalAnswer: finalAnswerTimeout,
	}
	p.sinkCtx = wirelog.WithSink(context.Background(), zapSink(logger))
	p.sink = server.PrepareServer(context.Background(), server.ConnectionOpts{
		// The collector's own deadlines, so a discarded agent sees the same
		// peer behavior as a real one.
		Timeout:              server.DefaultAgentTimeout,
		RotationPeriod:       uint64(cfg.RotationPeriod.Milliseconds()),
		RequiredRotationSize: cfg.RequiredRotationSize,
	}, discardListener{metrics: m})
	p.bufs.New = func() any {
		b := make([]byte, cfg.CopyBufferSize)
		return &b
	}
	return p, nil
}

// Start listens on the endpoint and accepts in the background until
// Shutdown. An accept failure after a successful bind is logged and retried.
func (p *Proxy) Start() error {
	ln, err := net.Listen("tcp", p.cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", p.cfg.Endpoint, err)
	}
	p.ln = ln
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.acceptLoop()
	}()
	return nil
}

// Addr returns the bound address, or nil before Start.
func (p *Proxy) Addr() net.Addr {
	if p.ln == nil {
		return nil
	}
	return p.ln.Addr()
}

func (p *Proxy) acceptLoop() {
	var backoff time.Duration
	for {
		c, err := p.ln.Accept()
		if err != nil {
			if p.isClosed() {
				return
			}
			// Only Shutdown ends the loop. Any other failure, such as running
			// out of descriptors, is retried, as net/http.Server.Serve does.
			backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
			p.logger.Warn("Accept failed; retrying", zap.Error(err), zap.Duration("backoff", backoff))
			time.Sleep(backoff)
			continue
		}
		backoff = 0
		select {
		case p.sem <- struct{}{}:
		default:
			// Refused by closing: the agent marks its dumper dead and
			// retries after its restart interval, which is the behavior
			// wanted over the cap.
			p.metrics.refused.Add(context.Background(), 1)
			p.logger.Debug("Refused an agent connection over max_connections",
				zap.Stringer("remote", c.RemoteAddr()), zap.Int("max_connections", p.cfg.MaxConnections))
			abort(c)
			continue
		}
		s := &session{client: c}
		if !p.track(s) {
			<-p.sem
			abort(c)
			return
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer func() { <-p.sem }()
			defer p.untrack(s)
			p.serve(s)
		}()
	}
}

func (p *Proxy) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func (p *Proxy) track(s *session) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.conns[s] = struct{}{}
	return true
}

func (p *Proxy) untrack(s *session) {
	p.mu.Lock()
	delete(p.conns, s)
	p.mu.Unlock()
}

// serve runs one agent connection to its end.
func (p *Proxy) serve(s *session) {
	defer s.close()
	hs, err := readHandshake(s.client, p.cfg.HandshakeTimeout)
	if err != nil {
		p.logger.Info("Closing an agent connection without a valid handshake",
			zap.Stringer("remote", s.client.RemoteAddr()), zap.Error(err))
		abort(s.client)
		return
	}
	s.mu.Lock()
	s.attrs = rules.Attrs{Namespace: hs.namespace, Service: hs.service, Pod: hs.pod}
	// Evaluated under s.mu, so a reload either sees this session decided or
	// has already swapped the rules this evaluation reads.
	s.mode = p.policy.Eval(s.attrs)
	s.decided = true
	mode := s.mode
	s.mu.Unlock()

	if p.logger.Core().Enabled(zap.DebugLevel) {
		p.logger.Debug("Agent connected", zap.String("namespace", hs.namespace), zap.String("service", hs.service),
			zap.String("pod", hs.pod), zap.String("mode", string(mode)))
	}
	if mode == rules.Discard || p.cfg.Upstream == "" {
		p.discard(s, hs.raw)
		return
	}
	p.passthrough(s, hs.raw)
}

// discard runs the collector protocol against the agent and drops the data.
// The handshake reply, stream handles, and acks are real, so the agent
// logs nothing and keeps its dumper alive.
func (p *Proxy) discard(s *session, prefix []byte) {
	ctx := context.Background()
	mode := modeAttr(string(rules.Discard))
	p.metrics.total.Add(ctx, 1, mode)
	p.metrics.active.Add(ctx, 1, mode)
	defer p.metrics.active.Add(ctx, -1, mode)
	p.sink.ServeConn(p.sinkCtx, newReplayConn(s.client, prefix))
}

// Reevaluate applies changed rules to open connections. A connection whose
// mode changed is reset, and the agent reconnects into the new mode after
// its restart interval, 10 seconds by default. It is the only way to move an
// agent from discard to passthrough: the upstream never saw that
// connection's handshake.
func (p *Proxy) Reevaluate() {
	p.mu.Lock()
	sessions := make([]*session, 0, len(p.conns))
	for s := range p.conns {
		sessions = append(sessions, s)
	}
	p.mu.Unlock()
	closed := 0
	for _, s := range sessions {
		s.mu.Lock()
		changed := s.decided && p.policy.Eval(s.attrs) != s.mode
		from, attrs := s.mode, s.attrs
		s.mu.Unlock()
		if !changed {
			continue
		}
		s.abort()
		closed++
		p.metrics.closedByReload.Add(context.Background(), 1, modeAttr(string(from)))
		p.logger.Info("Closed an agent connection whose mode changed; the agent reconnects in the new mode",
			zap.String("namespace", attrs.Namespace), zap.String("service", attrs.Service),
			zap.String("pod", attrs.Pod), zap.String("from", string(from)))
	}
	if closed > 0 {
		p.logger.Info("Applied the new rules to open agent connections", zap.Int("closed", closed))
	}
}

// passthrough dials the upstream and copies bytes between the two sockets in
// both directions. prefix holds bytes already read from the agent, sent
// upstream before the rest.
func (p *Proxy) passthrough(s *session, prefix []byte) {
	// The agent leg gets a FIN only when the agent finished first and both
	// copies ended cleanly; every other end resets it.
	fin := false
	defer func() {
		if !fin {
			abort(s.client)
		}
	}()
	ctx := context.Background()
	up, err := net.DialTimeout("tcp", p.cfg.Upstream, p.cfg.ConnectTimeout)
	if err != nil {
		p.metrics.UpstreamError(ctx)
		p.logger.Warn("Upstream unreachable; closing the agent connection, the agent retries after its restart interval",
			zap.String("upstream", p.cfg.Upstream), zap.Stringer("remote", s.client.RemoteAddr()), zap.Error(err))
		return
	}
	if !s.setUpstream(up) {
		return
	}

	mode := modeAttr(string(rules.Passthrough))
	p.metrics.total.Add(ctx, 1, mode)
	p.metrics.active.Add(ctx, 1, mode)
	defer p.metrics.active.Add(ctx, -1, mode)

	toUpstream := countingWriter{w: up, metrics: p.metrics}
	// agentEOF is set before the half-close, so an upstream that closes in
	// answer to it always finds the flag set.
	var agentEOF atomic.Bool
	agentDone := make(chan error, 1)
	go func() {
		if len(prefix) > 0 {
			if _, err := toUpstream.Write(prefix); err != nil {
				agentDone <- err
				return
			}
		}
		err := p.copy(toUpstream, s.client)
		if err == nil {
			agentEOF.Store(true)
			// Half-close, so the upstream sees the end of the stream. If the
			// upstream keeps its side open past finalAnswer, the reverse copy
			// fails with a timeout and the session ends with a reset.
			_ = up.SetReadDeadline(time.Now().Add(p.finalAnswer))
			err = up.(*net.TCPConn).CloseWrite()
		}
		agentDone <- err
	}()
	upDone := make(chan error, 1)
	go func() { upDone <- p.copy(s.client, up) }()

	// The agent finishing cleanly waits up to finalAnswer for the upstream to
	// finish its reply.
	// The upstream ending ends the session: nothing can answer the agent any
	// more, and the agent may leave its old socket open after a read timeout,
	// which would keep this session tracked forever. An error ends both.
	var upstreamEnded bool
	select {
	case err = <-agentDone:
		if err == nil {
			err = <-upDone
			fin = err == nil
			upstreamEnded = failedOn(err, "read")
		} else {
			// This copy writes only to the upstream.
			upstreamEnded = failedOn(err, "write")
		}
	case err = <-upDone:
		// The upstream can answer the half-close before agentDone is sent,
		// and select picks at random when both are ready.
		if err == nil && agentEOF.Load() {
			fin = <-agentDone == nil
		} else {
			// This copy reads only from the upstream.
			upstreamEnded = err == nil || failedOn(err, "read")
		}
	}
	if upstreamEnded {
		p.metrics.UpstreamError(ctx)
		s.mu.Lock()
		attrs := s.attrs
		s.mu.Unlock()
		p.logger.Debug("The upstream ended a passthrough session; resetting the agent connection",
			zap.String("namespace", attrs.Namespace), zap.String("service", attrs.Service),
			zap.String("pod", attrs.Pod), zap.Error(err))
	}
}

// failedOn reports whether err is a socket failure of op, "read" or "write".
// A socket the gateway closed itself, on a rules change or shutdown, is not a
// failure.
func failedOn(err error, op string) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == op && !errors.Is(err, net.ErrClosed)
}

// copy moves bytes until src ends. It returns nil on a clean end of src.
func (p *Proxy) copy(dst io.Writer, src io.Reader) error {
	buf := p.bufs.Get().(*[]byte)
	defer p.bufs.Put(buf)
	// Hide WriterTo and ReaderFrom, which net.TCPConn implements: with them
	// io.CopyBuffer ignores buf and allocates its own, and copy_buffer_size
	// would bound nothing.
	_, err := io.CopyBuffer(struct{ io.Writer }{dst}, struct{ io.Reader }{src}, *buf)
	return err
}

// Shutdown stops accepting and closes every agent connection at once. An
// agent connection lives as long as its pod and carries no resumable
// state, so waiting would only delay the agent's reconnect.
func (p *Proxy) Shutdown(ctx context.Context) error {
	if p.ln == nil {
		return nil
	}
	p.mu.Lock()
	p.closed = true
	err := p.ln.Close()
	sessions := make([]*session, 0, len(p.conns))
	for s := range p.conns {
		sessions = append(sessions, s)
	}
	p.mu.Unlock()
	for _, s := range sessions {
		s.abort()
	}
	p.sink.Stop()
	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
}

// zapSink writes libs/wire log records to logger at the matching level.
func zapSink(logger *zap.Logger) wirelog.Sink {
	return func(level, msg string) {
		switch level {
		case string(wirelog.ERROR):
			logger.Error(msg)
		case string(wirelog.WARNING):
			logger.Warn(msg)
		case string(wirelog.INFO):
			logger.Info(msg)
		default:
			logger.Debug(msg)
		}
	}
}
