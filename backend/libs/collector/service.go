// Package collector composes the Stage 1 write path: the agent TCP listener
// from libs/server feeding the hot store through the ingest listener. Seal
// loop, janitors, and the internal read API attach here in later Stage 1
// tasks (03-lifecycle.md §3.10).
package collector

import (
	"context"
	"errors"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotread"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/ingest"
	"github.com/Netcracker/qubership-profiler-backend/libs/server"
	"github.com/oklog/run"
)

type (
	// Options bundle the write-path configuration.
	Options struct {
		Store  hotstore.Config
		Server server.ConnectionOpts
		// ObjectStore is the S3 target of the upload loop (01-write-contract.md
		// §6.2). Nil disables uploads, mirroring the opt-in seal loop; the
		// collector app wiring passes an S3ObjectStore.
		ObjectStore hotstore.ObjectStore
		// InternalAPIAddr binds the /internal/v1 hot-read API
		// (02-read-contract.md §3; PROFILER_INTERNAL_API_PORT, default 8081).
		// Empty disables it, mirroring the opt-in loops; tests serve
		// hotread.New(store).Handler() themselves.
		InternalAPIAddr string
	}

	// Service is the running write path: exclusive PV owner plus TCP listener.
	Service struct {
		store    *hotstore.Store
		ingest   *ingest.Listener
		tcp      *server.Service
		uploader *hotstore.Uploader
		hotread  *hotread.API
		hotAddr  string
	}
)

// New opens the store, runs recovery (03-lifecycle.md §3), and prepares — but
// does not start — the TCP listener, mirroring the LOADING→RECOVERY states:
// no agent connection is accepted before recovery finishes.
func New(ctx context.Context, opts Options) (*Service, error) {
	store, err := hotstore.Open(opts.Store)
	if err != nil {
		return nil, err
	}
	if err := store.Recover(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}
	listener := ingest.NewListener(store)
	svc := &Service{
		store:  store,
		ingest: listener,
		tcp:    server.PrepareServer(ctx, opts.Server, listener),
	}
	if opts.ObjectStore != nil {
		svc.uploader = hotstore.NewUploader(store, opts.ObjectStore)
	}
	if opts.InternalAPIAddr != "" {
		svc.hotread = hotread.New(store)
		svc.hotAddr = opts.InternalAPIAddr
	}
	return svc, nil
}

// Store exposes the hot store for the read API and for tests.
func (s *Service) Store() *hotstore.Store { return s.store }

// Run serves agent connections until ctx is cancelled or the listener fails,
// then finalizes open pod-restarts and releases the PV. It follows the
// oklog/run all-in-one pattern of apps/dumps-collector.
func (s *Service) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var gr run.Group
	gr.Add(func() error {
		return s.tcp.Start(ctx)
	}, func(error) {
		s.tcp.Stop()
	})
	// The seal loop (01-write-contract.md §6.1) is opt-in until the collector
	// app wiring sets the interval; tests drive Store.Seal directly.
	if interval := s.store.Config().SealCheckInterval; interval > 0 {
		gr.Add(func() error {
			return s.store.RunSealLoop(ctx, interval)
		}, func(error) {
			cancel()
		})
	}
	// Same opt-in pattern for the upload loop (01 §6.2, 03 §3.8-§3.9); its
	// first tick re-triggers whatever a previous process left pending.
	if interval := s.store.Config().UploadCheckInterval; s.uploader != nil && interval > 0 {
		gr.Add(func() error {
			return s.uploader.Run(ctx, interval)
		}, func(error) {
			cancel()
		})
	}
	// The internal hot-read API (02-read-contract.md §3) serves for as long as
	// the store does; the app wiring sets the bind address.
	if s.hotread != nil {
		gr.Add(func() error {
			return s.hotread.Run(ctx, s.hotAddr)
		}, func(error) {
			cancel()
		})
	}
	gr.Add(func() error {
		<-ctx.Done()
		return ctx.Err()
	}, func(error) {
		cancel()
	})
	err := gr.Run()

	// Open pod-restarts close as on agent disconnect; the store then releases
	// the flock so the next collector process can recover the PV.
	s.ingest.Close(ctx)
	if closeErr := s.store.Close(); closeErr != nil && (err == nil || isContextErr(err)) {
		return closeErr
	}
	if isContextErr(err) {
		return nil // a requested shutdown is not a failure
	}
	return err
}

func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
