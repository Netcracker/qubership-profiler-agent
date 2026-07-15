package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	appenv "github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/envconfig"
	"github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/health"
	"github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/metrics"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotread"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	profio "github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/Netcracker/qubership-profiler-backend/libs/server"
	"github.com/oklog/run"
	pkgerrors "github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// Agent-socket deadlines. The read timeout must exceed the agent's keep-alive
// cadence so an idle-but-healthy connection is not dropped; the values mirror
// the integration suite's.
var agentTimeouts = profio.TcpTimeout{
	ConnectTimeout: 10 * time.Second,
	SessionTimeout: 60 * time.Second,
	ReadTimeout:    40 * time.Second,
	WriteTimeout:   2 * time.Second,
}

var collectCmd = &cobra.Command{
	Use:          "collect",
	Short:        "Run the collector write path: agent TCP, seal and upload loops, /internal/v1",
	RunE:         runCollect,
	SilenceUsage: true,
}

func init() {
	rootCmd.AddCommand(collectCmd)
}

func runCollect(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	cfg, err := appenv.ParseCollect()
	if err != nil {
		return err
	}
	ctx, err = log.SetLevelString(ctx, cfg.LogLevel)
	if err != nil {
		return err
	}

	// Bind the internal port before any heavy lifting so probes read
	// LOADING/RECOVERY instead of connection-refused (03-lifecycle.md §2);
	// the agent TCP listener stays down until recovery finishes. /metrics
	// rides the same port outside the gate, so a scrape works mid-recovery.
	gate := health.NewGate("/internal/v1")
	reg := metrics.NewRegistry()
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.InternalAPIPort))
	if err != nil {
		return pkgerrors.Wrap(err, "bind internal API")
	}
	internal := &http.Server{Handler: metrics.Mux(reg, gate)}
	serveErr := make(chan error, 1)
	go func() { serveErr <- internal.Serve(ln) }()

	fatal := func(stage string, err error) error {
		gate.Set(health.StateFatal, stage+": "+err.Error())
		_ = internal.Close()
		return pkgerrors.Wrap(err, stage)
	}

	gate.Set(health.StateLoading, "connecting to S3")
	s3params, err := cfg.S3.Params()
	if err != nil {
		return fatal("resolve S3 credentials", err)
	}
	mc, err := s3.NewClient(ctx, s3params)
	if err != nil {
		return fatal("connect to S3", err)
	}

	replica := cfg.Replica
	if replica == "" {
		replica = os.Getenv("HOSTNAME") // the pod name under k8s (04 §3.2)
	}

	gate.Set(health.StateRecovery, "recovering the hot store")
	svc, err := collector.New(ctx, collector.Options{
		Store: hotstore.Config{
			DataDir:               cfg.DataDir,
			TimeBucket:            cfg.TimeBucket,
			TimeBucketGrace:       cfg.TimeBucketGrace,
			DictFsyncRecords:      cfg.DictFsyncRecords,
			DictFsyncInterval:     cfg.DictFsyncInterval,
			DurationThresholds:    [2]time.Duration(cfg.DurationThresholds),
			Replica:               replica,
			SealCheckInterval:     cfg.SealCheckInterval,
			UploadCheckInterval:   cfg.UploadCheckInterval,
			JanitorCheckInterval:  cfg.JanitorCheckInterval,
			HotRetention:          cfg.HotRetention,
			ChunksStagingMaxBytes: int64(cfg.ChunksStagingMaxBytes),
			WalPurgeGrace:         cfg.WalPurgeGrace,
		},
		Server: server.ConnectionOpts{
			ProtocolPort:         cfg.AgentPort,
			Timeout:              agentTimeouts,
			RequiredRotationSize: uint64(cfg.SegmentRotationSize),
		},
		ObjectStore: collector.NewS3ObjectStore(mc),
	})
	if err != nil {
		return fatal("recover the hot store", err)
	}

	metrics.RegisterCollect(reg, svc.Store(), svc.Uploader())
	gate.Mount(hotread.New(svc.Store()).Handler())
	gate.Set(health.StateReady, "")
	log.Info(ctx, "collector ready: agent :%d, internal API :%d, data dir %s",
		cfg.AgentPort, cfg.InternalAPIPort, cfg.DataDir)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var gr run.Group
	gr.Add(func() error {
		return svc.Run(runCtx)
	}, func(error) {
		gate.Set(health.StateTerminating, "finalizing pod-restarts")
		cancel()
	})
	gr.Add(func() error {
		select {
		case err := <-serveErr:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-runCtx.Done():
			return nil
		}
	}, func(error) {
		shCtx, shCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shCancel()
		_ = internal.Shutdown(shCtx)
	})
	gr.Add(signalActor(runCtx, gate, cfg.ShutdownDrainGrace))
	return gr.Run()
}
