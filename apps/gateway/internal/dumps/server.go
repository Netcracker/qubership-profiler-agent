// Package dumps proxies diagtools dump uploads to the dumps collector, or
// accepts and discards them. It has no Collector imports.
//
// The client's retry behavior shapes every decision here. diagtools keeps a
// file and resends it from byte zero on any answer other than 200, 201, or
// 204, for up to 48 hours. So the gateway never sheds load with an error
// status: an upload it will not forward is read to the end and answered 204,
// and the client deletes its copy. An upstream outage in passthrough answers
// 502, so the client keeps the file and retries.
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
		ErrorLog:          zap.NewStdLog(logger.Named("http")),
	}
	return s, nil
}

func (s *Server) newProxy() *httputil.ReverseProxy {
	upstream := s.cfg.Upstream
	transport := &http.Transport{
		// A dump goes straight to the collector, never through an
		// environment proxy.
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   s.cfg.ConnectTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
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
	if !slices.Contains(s.cfg.AllowedMethods, r.Method) {
		w.Header().Set("Allow", strings.Join(s.cfg.AllowedMethods, ", "))
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	attrs := parsePath(r.URL.Path, s.cfg.PathPrefix)

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
	limit := s.limit(attrs.DumpType)
	if r.ContentLength > limit {
		// A declared length is the one case where the cap can be enforced
		// before the bytes cross the network to the upstream.
		s.discard(w, r, attrs.DumpType, outcomeOverCap)
		return
	}
	body := newCapBody(r.Body, limit)
	r.Body = body
	st := &requestState{body: body, dumpType: attrs.DumpType}
	s.proxy.ServeHTTP(w, r.WithContext(context.WithValue(ctx, ctxKey{}, st)))
}

func (s *Server) limit(dumpType string) int64 {
	if l, ok := s.cfg.MaxBodySizePerType[dumpType]; ok {
		return l
	}
	return s.cfg.MaxBodySize
}

// discard reads the body to the end and answers 204, so the client marks
// the file uploaded and deletes its copy. The read comes first: answering
// before the body ends could surface as a broken pipe in the client.
func (s *Server) discard(w http.ResponseWriter, r *http.Request, dumpType, outcome string) {
	n, err := io.Copy(io.Discard, r.Body)
	if err != nil {
		s.metrics.upload(r.Context(), dumpType, outcomeClientAborted, n)
		s.logger.Debug("Client aborted a discarded upload", zap.String("path", r.URL.Path), zap.Error(err))
		return
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
	_, _ = st.body.drain()
	bs := st.body.state()
	outcome := outcomeForwarded
	if bs.clientErr != nil {
		outcome = outcomeClientAborted
	}
	s.metrics.upload(resp.Request.Context(), st.dumpType, outcome, bs.total)
	return nil
}

// proxyError handles a failed upstream exchange: an upload over the size
// cap, a client that went away, or an upstream that failed.
func (s *Server) proxyError(w http.ResponseWriter, r *http.Request, err error) {
	st := r.Context().Value(ctxKey{}).(*requestState)
	ctx := r.Context()
	before := st.body.state()
	if before.overCap {
		if _, derr := st.body.drain(); derr != nil {
			s.metrics.upload(ctx, st.dumpType, outcomeClientAborted, st.body.state().total)
			return
		}
		s.metrics.upload(ctx, st.dumpType, outcomeOverCap, st.body.state().total)
		s.logger.Info("Discarded an upload over the size cap",
			zap.String("path", r.URL.Path), zap.Int64("limit", s.limit(st.dumpType)))
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if before.clientErr != nil {
		s.metrics.upload(ctx, st.dumpType, outcomeClientAborted, before.total)
		s.logger.Debug("Client aborted an upload", zap.String("path", r.URL.Path), zap.Error(before.clientErr))
		return
	}
	s.metrics.upstreamErrors.Add(ctx, 1, metric.WithAttributes(surfaceAttr))
	if _, derr := st.body.drain(); derr != nil {
		s.metrics.upload(ctx, st.dumpType, outcomeClientAborted, st.body.state().total)
		return
	}
	s.metrics.upload(ctx, st.dumpType, outcomeUpstreamError, st.body.state().total)
	s.logger.Warn("Upstream failed; answering 502 so the client keeps the file and retries",
		zap.String("path", r.URL.Path), zap.Error(err))
	w.WriteHeader(http.StatusBadGateway)
}

// RecordRulesReload counts a rules file load.
func (s *Server) RecordRulesReload(ok bool) {
	result := "success"
	if !ok {
		result = "failure"
	}
	s.metrics.rulesReloaded(result)
}
