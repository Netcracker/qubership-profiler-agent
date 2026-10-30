// Package tcp carries profiler agent connections to the collector, or
// terminates and discards them. It has no Collector imports.
//
// Every connection opens with the agent's handshake, which names its
// namespace, service, and pod. The proxy reads it, picks the mode from the
// rules, and then either replays it to the upstream and splices the
// sockets, or hands the connection to a libs/wire server that runs the
// protocol and stores nothing.
//
// The agent's own retry shapes the design. A connection it cannot
// establish, or one that drops, makes the agent mark its dumper dead, log
// one warning, drop its buffers, and retry every 10 seconds
// (DUMPER_RESTART_INTERVAL). So the proxy refuses over its connection cap
// and on an unreachable upstream by closing the socket: the agent copes
// with that on its own.
package tcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	profio "github.com/Netcracker/qubership-profiler-agent/libs/wire/io"
	"github.com/Netcracker/qubership-profiler-agent/libs/wire/server"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
)

// Config is the validated configuration of a Proxy.
type Config struct {
	// Endpoint is the listen address, host:port.
	Endpoint string
	// Upstream is the collector's host:port. Empty is allowed only when
	// every decision is discard.
	Upstream       string
	ConnectTimeout time.Duration
	MaxConnections int
	// CopyBufferSize is the splice buffer per direction.
	CopyBufferSize int
	// HandshakeTimeout bounds the wait for the agent's handshake.
	HandshakeTimeout time.Duration
	// RotationPeriod and RequiredRotationSize are the INIT_STREAM_V2 reply
	// fields a discarding connection sends the agent.
	RotationPeriod       time.Duration
	RequiredRotationSize uint64
}

// discardTimeout matches the collector's agent socket timeouts
// (apps/profiler-backend/cmd/collect.go), so a discarded agent sees the same
// peer behavior as a real one.
var discardTimeout = profio.TcpTimeout{
	SessionTimeout: 60 * time.Second,
	ReadTimeout:    40 * time.Second,
	WriteTimeout:   2 * time.Second,
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

	ln     net.Listener
	mu     sync.Mutex
	conns  map[*session]struct{}
	closed bool
	wg     sync.WaitGroup
}

// session is one agent connection and, in passthrough, its upstream leg.
type session struct {
	client net.Conn
	mu     sync.Mutex
	// upstream, mode, attrs, and decided are guarded by mu. decided turns
	// true once the mode is chosen; Reevaluate skips sessions still reading
	// their handshake, which will see the new rules when they decide.
	upstream net.Conn
	mode     rules.Mode
	attrs    rules.Attrs
	decided  bool
}

// close closes both legs. It is safe to call more than once.
func (s *session) close() {
	_ = s.client.Close()
	s.mu.Lock()
	up := s.upstream
	s.mu.Unlock()
	if up != nil {
		_ = up.Close()
	}
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
	}
	p.sink = server.PrepareServer(context.Background(), server.ConnectionOpts{
		Timeout:              discardTimeout,
		RotationPeriod:       uint64(cfg.RotationPeriod.Milliseconds()),
		RequiredRotationSize: cfg.RequiredRotationSize,
	}, discardListener{discarded: m.bytesDiscarded})
	p.bufs.New = func() any {
		b := make([]byte, cfg.CopyBufferSize)
		return &b
	}
	return p, nil
}

// Start listens on the endpoint and accepts in the background. An accept
// failure after a successful bind is passed to onFatal.
func (p *Proxy) Start(onFatal func(error)) error {
	ln, err := net.Listen("tcp", p.cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", p.cfg.Endpoint, err)
	}
	p.ln = ln
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := p.acceptLoop(); err != nil {
			onFatal(fmt.Errorf("accept agent connections on %s: %w", p.cfg.Endpoint, err))
		}
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

func (p *Proxy) acceptLoop() error {
	var backoff time.Duration
	for {
		c, err := p.ln.Accept()
		if err != nil {
			if p.isClosed() {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				// Out of descriptors or a similar transient failure.
				backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
				p.logger.Warn("Accept failed; retrying", zap.Error(err), zap.Duration("backoff", backoff))
				time.Sleep(backoff)
				continue
			}
			return err
		}
		backoff = 0
		select {
		case p.sem <- struct{}{}:
		default:
			// Refused by closing: the agent marks its dumper dead and
			// retries in 10 s, which is the behavior wanted over the cap.
			p.metrics.refused.Add(context.Background(), 1)
			p.logger.Debug("Refused an agent connection over max_connections",
				zap.Stringer("remote", c.RemoteAddr()), zap.Int("max_connections", p.cfg.MaxConnections))
			_ = c.Close()
			continue
		}
		s := &session{client: c}
		if !p.track(s) {
			<-p.sem
			_ = c.Close()
			return nil
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
	p.sink.ServeConn(ctx, newReplayConn(s.client, prefix))
}

// Reevaluate applies changed rules to open connections. A connection whose
// mode changed is closed; the agent reconnects within 10 s into the new
// mode. It is the only way to move an agent from discard to passthrough:
// the upstream never saw that connection's handshake.
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
		s.close()
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

// passthrough dials the upstream and splices the two sockets. prefix holds
// bytes already read from the agent, sent upstream before the rest.
func (p *Proxy) passthrough(s *session, prefix []byte) {
	ctx := context.Background()
	up, err := net.DialTimeout("tcp", p.cfg.Upstream, p.cfg.ConnectTimeout)
	if err != nil {
		p.metrics.upstreamErrors.Add(ctx, 1, metric.WithAttributes(surfaceAttr))
		p.logger.Warn("Upstream unreachable; closing the agent connection, the agent retries in 10 s",
			zap.String("upstream", p.cfg.Upstream), zap.Stringer("remote", s.client.RemoteAddr()), zap.Error(err))
		return
	}
	s.mu.Lock()
	s.upstream = up
	s.mu.Unlock()
	if p.isClosed() {
		return
	}

	mode := modeAttr(string(rules.Passthrough))
	p.metrics.total.Add(ctx, 1, mode)
	p.metrics.active.Add(ctx, 1, mode)
	defer p.metrics.active.Add(ctx, -1, mode)

	toUpstream := countingWriter{w: up, counter: p.metrics.bytesForwarded}
	errc := make(chan error, 2)
	go func() {
		if len(prefix) > 0 {
			if _, err := toUpstream.Write(prefix); err != nil {
				errc <- err
				return
			}
		}
		errc <- p.copy(toUpstream, s.client, up)
	}()
	go func() { errc <- p.copy(s.client, up, s.client) }()

	// One side finishing cleanly half-closes the other and waits for it.
	// An error on either side ends both.
	if err := <-errc; err != nil {
		return
	}
	<-errc
}

// copy moves bytes until src ends, then half-closes dst so the peer sees
// the end of the stream. It returns nil on a clean end of src.
func (p *Proxy) copy(dst io.Writer, src io.Reader, dstConn net.Conn) error {
	buf := p.bufs.Get().(*[]byte)
	defer p.bufs.Put(buf)
	// Hide WriterTo and ReaderFrom, which net.TCPConn implements: with them
	// io.CopyBuffer ignores buf and allocates its own, and copy_buffer_size
	// would bound nothing.
	_, err := io.CopyBuffer(struct{ io.Writer }{dst}, struct{ io.Reader }{src}, *buf)
	if err != nil {
		return err
	}
	if cw, ok := dstConn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return dstConn.Close()
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
		s.close()
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
