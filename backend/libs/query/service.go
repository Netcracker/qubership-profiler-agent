package query

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/oklog/run"
)

type (
	// Options bundle the read-path configuration. The hot fan-out options
	// (COLLECTOR_HEADLESS_SVC discovery, per-replica timeouts; 02 §7) attach
	// in the fan-out slice.
	Options struct {
		Config Config
		// ColdStore is the S3 surface of the cold tier (02 §5). Production
		// wiring passes NewS3ObjectReader; tests pass a fake.
		ColdStore cold.ObjectStore
	}

	// Service is the running query API: stateless, no PV (02 §1).
	Service struct {
		cfg  Config
		cold *cold.Source
		echo *echo.Echo
	}
)

// New composes the query service; it binds nothing until Run.
func New(opts Options) *Service {
	cfg := opts.Config.Normalize()
	s := &Service{
		cfg:  cfg,
		cold: &cold.Source{Store: opts.ColdStore, ListConcurrency: cfg.ListConcurrency},
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
