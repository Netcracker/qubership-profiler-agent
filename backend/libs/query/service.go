package query

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/hot"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/oklog/run"
)

type (
	// Options bundle the read-path configuration.
	Options struct {
		Config Config
		// ColdStore is the S3 surface of the cold tier (02 §5). Production
		// wiring passes NewS3ObjectReader; tests pass a fake.
		ColdStore cold.ObjectStore
		// HotDiscovery overrides the replica discovery of the hot tier (02
		// §7.1). Nil falls back to DNS over Config.CollectorService; with that
		// empty too the query stays cold-only.
		HotDiscovery hot.Discovery
		// Metrics receives the query Prometheus series; nil disables them.
		Metrics *Metrics
		// UI is the built single-page app to serve at /ui (07 §6); nil
		// leaves the route unregistered. Production wiring passes
		// apps/ui.Dist(); tests pass an fstest.MapFS.
		UI fs.FS
	}

	// Service is the running query API: stateless, no PV (02 §1). The
	// dictionary cache is a revalidation shortcut, not state — losing it only
	// costs refetches.
	Service struct {
		cfg       Config
		cold      *cold.Source
		hot       *hot.Client
		discovery hot.Discovery
		dicts     *dictCache
		echo      *echo.Echo
		metrics   *Metrics
		ui        fs.FS
		// URL prefix the SPA is served under (07 §6), "" for a root build or
		// e.g. "/ui"; read from the built index.html so serving follows the base.
		uiPrefix string
	}
)

// New composes the query service; it binds nothing until Run.
func New(opts Options) *Service {
	cfg := opts.Config.Normalize()
	coldStore := opts.ColdStore
	if opts.Metrics != nil {
		coldStore = countingColdStore{inner: coldStore, metrics: opts.Metrics}
	}
	s := &Service{
		cfg: cfg,
		cold: &cold.Source{Store: coldStore, ListConcurrency: cfg.ListConcurrency,
			DurationThresholds: cfg.DurationThresholds},
		dicts:   newDictCache(),
		metrics: opts.Metrics,
	}
	s.ui = opts.UI
	if s.ui != nil {
		s.uiPrefix = uiPrefix(s.ui)
	}
	s.discovery = opts.HotDiscovery
	if s.discovery == nil && cfg.CollectorService != "" {
		s.discovery = hot.DNSDiscovery{Service: cfg.CollectorService, Port: cfg.CollectorPort}
	}
	if s.discovery != nil {
		s.hot = hot.NewClient(cfg.FanoutTimeout)
	}
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogStatus:   true,
		LogURI:      true,
		LogError:    true,
		HandleError: true,
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
			ctx := c.Request().Context()
			switch {
			case v.Error == nil:
				log.Debug(ctx, "request, uri = %s, status = %d", v.URI, v.Status)
			case isClientSideError(v.Error):
				// A routine 4xx, a canceled/timed-out request, or the caller
				// closing its socket mid-response: expected traffic, not a
				// server-side failure (PR 708 review #23).
				log.Debug(ctx, "request error (client-side), uri = %s, status = %d: %v", v.URI, v.Status, v.Error)
			default:
				log.Error(ctx, v.Error, "request error, uri = %s, status = %d", v.URI, v.Status)
			}
			return nil
		},
	}))
	s.routes(e)
	s.echo = e
	return s
}

// Handler exposes the HTTP surface for tests and embedding.
func (s *Service) Handler() http.Handler { return s.echo }

// Run serves /api/v1 until ctx is cancelled, following the oklog/run
// pattern of libs/collector.
func (s *Service) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.echo.Server.BaseContext = func(net.Listener) context.Context { return ctx }
	var gr run.Group
	gr.Add(func() error {
		err := s.echo.Start(s.cfg.ListenAddr)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}, func(error) {
		_ = s.echo.Shutdown(context.Background())
	})
	gr.Add(func() error {
		<-ctx.Done()
		return ctx.Err()
	}, func(error) {
		cancel()
	})
	err := gr.Run()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil // a requested shutdown is not a failure
	}
	return err
}

// isClientSideError reports a request outcome the client caused, not the
// server: a routine 4xx (not-found, bad request), a canceled or timed-out
// request context, or the caller closing its socket mid-response. None of
// these need operator attention, unlike a real 5xx or an unexpected error
// (PR 708 review #23).
func isClientSideError(err error) bool {
	var httpErr *echo.HTTPError
	if errors.As(err, &httpErr) && httpErr.Code >= 400 && httpErr.Code < 500 {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return isBrokenPipeOrReset(err)
}

// isBrokenPipeOrReset detects a client that closed its connection while the
// server was still writing the response.
func isBrokenPipeOrReset(err error) bool {
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		var sysErr *os.SyscallError
		if errors.As(netErr.Err, &sysErr) {
			return errors.Is(sysErr.Err, syscall.EPIPE) || errors.Is(sysErr.Err, syscall.ECONNRESET)
		}
	}
	// Some write paths surface only a formatted string with no syscall
	// wrapper to match against — fall back to the message.
	msg := err.Error()
	return strings.Contains(msg, "broken pipe") || strings.Contains(msg, "connection reset by peer")
}
