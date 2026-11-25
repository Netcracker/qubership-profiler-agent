// Package dumps proxies diagtools dump uploads to the dumps collector, or
// accepts and discards them. It has no Collector imports.
//
// The client's retry behavior shapes every decision here. diagtools keeps a
// queued dump file and resends it from byte zero on any answer other than
// 200, 201, or 204, until DIAGNOSTIC_UPLOAD_MAX_AGE passes (48 hours by
// default). So the gateway never sheds load with an error status: an upload
// it will not forward is read to the end and answered 204, and the client
// deletes its copy. An upstream outage in passthrough answers 502, so the
// client keeps a queued file and retries. A thread dump that diagtools sends
// from memory has no file, and a 502 loses it.
package dumps

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
)

// Config is the validated configuration of a Server.
type Config struct {
	// Endpoint is the listen address, host:port.
	Endpoint string
	// Upstream is the dumps collector's base URL. Nil is allowed only when
	// every decision is discard.
	Upstream              *url.URL
	PathPrefix            string
	AllowedMethods        []string
	ConnectTimeout        time.Duration
	ResponseHeaderTimeout time.Duration
	ReadHeaderTimeout     time.Duration
	// IdleTimeout bounds a pause in body progress: a client read or an
	// upstream write. Zero disables it.
	IdleTimeout time.Duration
	// MaxBodySize caps an upload in bytes; MaxBodySizePerType overrides it
	// per dump type.
	MaxBodySize          int64
	MaxBodySizePerType   map[string]int64
	MaxConcurrentUploads int
	// DrainTimeout bounds how long Shutdown waits for in-flight uploads.
	DrainTimeout time.Duration
}

// Server is the dump-upload listener.
type Server struct {
	cfg     Config
	policy  *rules.Policy
	logger  *zap.Logger
	metrics *metrics
	sem     chan struct{}
	proxy   *httputil.ReverseProxy
	srv     *http.Server
	ln      net.Listener
	serveWG sync.WaitGroup
}

type ctxKey struct{}

type requestState struct {
	body     *capBody
	dumpType string
}

// New builds a Server. It allocates the proxy but opens no socket.
func New(cfg Config, policy *rules.Policy, logger *zap.Logger, meter metric.Meter) (*Server, error) {
	m, err := newMetrics(meter)
	if err != nil {
		return nil, fmt.Errorf("create dump metrics: %w", err)
	}
	s := &Server{
		cfg:     cfg,
		policy:  policy,
		logger:  logger,
		metrics: m,
		sem:     make(chan struct{}, cfg.MaxConcurrentUploads),
	}
	if cfg.Upstream != nil {
		s.proxy = s.newProxy()
	}
	s.srv = &http.Server{
		Handler: s,
		// Only the header has a deadline. http.Server.ReadTimeout would
		// also cover the body and cut off a large dump mid-transfer.
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		// Without it a keep-alive connection waits for its next request
		// forever: ReadTimeout is zero, so net/http has no fallback.
		// diagtools builds a new transport per file and never reuses one.
		IdleTimeout: cfg.IdleTimeout,
		ErrorLog:    zap.NewStdLog(logger.Named("http")),
	}
	return s, nil
}

// dial connects to the upstream and bounds each write by IdleTimeout.
func (s *Server) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: s.cfg.ConnectTimeout, KeepAlive: 30 * time.Second}
	conn, err := d.DialContext(ctx, network, addr)
	if err != nil || s.cfg.IdleTimeout <= 0 {
		return conn, err
	}
	return &writeIdleConn{Conn: conn, timeout: s.cfg.IdleTimeout}, nil
}

func (s *Server) newProxy() *httputil.ReverseProxy {
	upstream := s.cfg.Upstream
	transport := &http.Transport{
		// A dump goes straight to the collector, never through an
		// environment proxy.
		Proxy:       nil,
		DialContext: s.dial,
		// An https upstream that accepts TCP and stalls the handshake would
		// otherwise hold the slot: no body or response deadline runs yet.
		TLSHandshakeTimeout: s.cfg.ConnectTimeout,
		// Counted from the end of the request body, so a long upload does
		// not run into it. There is no whole-request timeout: the client
		// already has one.
		ResponseHeaderTimeout: s.cfg.ResponseHeaderTimeout,
		DisableCompression:    true,
		MaxIdleConnsPerHost:   s.cfg.MaxConcurrentUploads,
		IdleConnTimeout:       90 * time.Second,
	}
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			// Keeps the request URI, which carries every routing input,
			// and sets Host to the upstream's, as nginx proxy_pass does.
			pr.SetURL(upstream)
		},
		Transport:      transport,
		ModifyResponse: s.modifyResponse,
		ErrorHandler:   s.proxyError,
		ErrorLog:       zap.NewStdLog(s.logger.Named("proxy")),
	}
}

// Start listens on the endpoint and serves in the background. A serve
// failure after a successful bind is passed to onFatal.
func (s *Server) Start(onFatal func(error)) error {
	ln, err := net.Listen("tcp", s.cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.Endpoint, err)
	}
	s.ln = ln
	s.serveWG.Add(1)
	go func() {
		defer s.serveWG.Done()
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			onFatal(fmt.Errorf("serve dump uploads on %s: %w", s.cfg.Endpoint, err))
		}
	}()
	return nil
}

// Addr returns the bound address, or nil before Start.
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Shutdown stops accepting uploads and waits up to DrainTimeout for the
// ones in flight, then closes the rest.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.ln == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.DrainTimeout)
	defer cancel()
	err := s.srv.Shutdown(ctx)
	if err != nil {
		err = errors.Join(err, s.srv.Close())
	}
	s.serveWG.Wait()
	return err
}

// ServeHTTP handles one upload, delete, or stray request.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !underPrefix(r.URL.Path, s.cfg.PathPrefix) {
		http.NotFound(w, r)
		return
	}
	if !canonicalPath(r.URL) {
		// The upstream would resolve such a path to another source, or to a
		// location outside the prefix, than the rules evaluated. diagtools
		// never builds one.
		http.Error(w, "non-canonical path", http.StatusBadRequest)
		return
	}
	if !slices.Contains(s.cfg.AllowedMethods, r.Method) {
		w.Header().Set("Allow", strings.Join(s.cfg.AllowedMethods, ", "))
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	attrs := parsePath(r.URL.Path, s.cfg.PathPrefix)
	if s.cfg.IdleTimeout > 0 {
		r.Body = &idleBody{ReadCloser: r.Body, rc: http.NewResponseController(w), timeout: s.cfg.IdleTimeout}
	}

	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	default:
		// Queuing would hold the client past its deadline; an error status
		// would make it resend the same file every tick.
		s.discard(w, r, attrs.DumpType, outcomeOverConcurrency)
		return
	}
	ctx := r.Context()
	s.metrics.inFlight.Add(ctx, 1)
	defer s.metrics.inFlight.Add(ctx, -1)

	if s.policy.Eval(attrs) == rules.Discard || s.proxy == nil {
		s.discard(w, r, attrs.DumpType, outcomeDiscarded)
		return
	}
	limit, _ := s.limit(attrs.DumpType)
	if r.ContentLength > limit {
		// A declared length is the one case where the cap can be enforced
		// before the bytes cross the network to the upstream.
		s.logOverCap(r, attrs.DumpType)
		s.discard(w, r, attrs.DumpType, outcomeOverCap)
		return
	}
	body := newCapBody(r.Body, limit)
	r.Body = body
	st := &requestState{body: body, dumpType: attrs.DumpType}
	s.proxy.ServeHTTP(w, r.WithContext(context.WithValue(ctx, ctxKey{}, st)))
}

// limit returns the size cap for dumpType and the config key that sets it.
func (s *Server) limit(dumpType string) (int64, string) {
	if l, ok := s.cfg.MaxBodySizePerType[dumpType]; ok {
		return l, "max_body_size_per_type." + dumpType
	}
	return s.cfg.MaxBodySize, "max_body_size"
}

// logOverCap logs an upload discarded over its size cap, with the key that
// set the cap.
func (s *Server) logOverCap(r *http.Request, dumpType string) {
	limit, key := s.limit(dumpType)
	s.logger.Info("Discarded an upload over the size cap", zap.String("path", r.URL.Path),
		zap.String("dump_type", dumpType), zap.String("setting", key), zap.Int64("limit", limit))
}

// discard reads the body to the end and answers 204, so the client marks
// the file uploaded and deletes its copy. The read comes first: answering
// before the body ends could surface as a broken pipe in the client.
func (s *Server) discard(w http.ResponseWriter, r *http.Request, dumpType, outcome string) {
	n, err := io.Copy(io.Discard, r.Body)
	if err != nil {
		s.metrics.upload(r.Context(), dumpType, outcomeClientAborted, n)
		s.logger.Debug("Client aborted a discarded upload", zap.String("path", r.URL.Path), zap.Error(err))
		abortClient()
	}
	s.metrics.upload(r.Context(), dumpType, outcome, n)
	w.WriteHeader(http.StatusNoContent)
}

// modifyResponse runs when the upstream has answered. If the upstream
// answered before the client finished sending, the rest of the body is
// drained first, so the client always gets its answer after its last byte.
// The upstream's status goes to the client unchanged, 405 on DELETE included.
func (s *Server) modifyResponse(resp *http.Response) error {
	st := resp.Request.Context().Value(ctxKey{}).(*requestState)
	if _, err := st.body.drain(); err != nil {
		// proxyError counts it and drops the connection.
		return err
	}
	state := st.body.state()
	if state.overCap {
		// The upstream answered before the body passed the cap. proxyError
		// answers 204, so the client stops resending an oversized file.
		return errOverCap
	}
	ctx := resp.Request.Context()
	if !clientAccepts(resp.Request.Method, resp.StatusCode) {
		s.metrics.upload(ctx, st.dumpType, outcomeUpstreamRejected, state.total)
		return nil
	}
	s.metrics.upload(ctx, st.dumpType, outcomeForwarded, state.total)
	// Bytes drained after an early answer never reached the upstream.
	s.metrics.Forwarded(ctx, state.sent)
	return nil
}

// clientAccepts reports whether diagtools treats the upstream's status as a
// success (apps/agent/diagtools/utils/sender.go): 200, 201, or 204 for an
// upload, and 200, 204, 404, or 405 for a DELETE.
func clientAccepts(method string, status int) bool {
	switch status {
	case http.StatusOK, http.StatusNoContent:
		return true
	case http.StatusCreated:
		return method != http.MethodDelete
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		return method == http.MethodDelete
	}
	return false
}

// proxyError handles a failed upstream exchange: an upload over the size
// cap, a client that went away, or an upstream that failed. The body is
// drained first, so the cap applies to the full size, also when the
// upstream failed before the body reached the cap.
func (s *Server) proxyError(w http.ResponseWriter, r *http.Request, err error) {
	st := r.Context().Value(ctxKey{}).(*requestState)
	ctx := r.Context()
	if before := st.body.state(); before.clientErr != nil {
		s.metrics.upload(ctx, st.dumpType, outcomeClientAborted, before.total)
		s.logger.Debug("Client aborted an upload", zap.String("path", r.URL.Path), zap.Error(before.clientErr))
		abortClient()
	}
	if _, derr := st.body.drain(); derr != nil {
		s.metrics.upload(ctx, st.dumpType, outcomeClientAborted, st.body.state().total)
		abortClient()
	}
	after := st.body.state()
	if ctx.Err() != nil {
		// The client closed its connection after sending the body, and
		// net/http canceled the upstream request.
		s.metrics.upload(ctx, st.dumpType, outcomeClientAborted, after.total)
		s.logger.Debug("Client went away before the upstream answered", zap.String("path", r.URL.Path), zap.Error(err))
		abortClient()
	}
	if after.overCap {
		s.metrics.upload(ctx, st.dumpType, outcomeOverCap, after.total)
		s.logOverCap(r, st.dumpType)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.metrics.UpstreamError(ctx)
	s.metrics.upload(ctx, st.dumpType, outcomeUpstreamError, after.total)
	s.logger.Warn("Upstream failed; answering 502. diagtools keeps a queued dump file and retries it, "+
		"but loses a thread dump it sent from memory", zap.String("path", r.URL.Path), zap.Error(err))
	w.WriteHeader(http.StatusBadGateway)
}

// abortClient drops the client connection without an answer. Any status,
// even the 200 that net/http writes by default, could make the client
// delete a file that was never stored.
func abortClient() { panic(http.ErrAbortHandler) }

// RecordRulesReload counts a rules file load.
func (s *Server) RecordRulesReload(ok bool) { s.metrics.RulesReloaded(ok) }
