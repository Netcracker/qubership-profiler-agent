package query

import (
	"context"
	"errors"
	"net"
	"net/http"

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
	}
)

// New composes the query service; it binds nothing until Run.
func New(opts Options) *Service {
	cfg := opts.Config.Normalize()
	s := &Service{
		cfg:   cfg,
		cold:  &cold.Source{Store: opts.ColdStore, ListConcurrency: cfg.ListConcurrency},
		dicts: newDictCache(),
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
			if v.Error != nil {
				log.Error(ctx, v.Error, "request error, uri = %s, status = %d", v.URI, v.Status)
			} else {
				log.Debug(ctx, "request, uri = %s, status = %d", v.URI, v.Status)
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
