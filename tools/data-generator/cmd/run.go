package cmd

import (
	"context"
	"os"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/tools/data-generator/pkg/data"
	"github.com/Netcracker/qubership-profiler-backend/tools/data-generator/pkg/s3"
	"github.com/Netcracker/qubership-profiler-backend/tools/data-generator/pkg/worker"
	"github.com/spf13/cobra"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/parser"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
)

const (
	SaveParsedFiles = false                                        // save separate files during parsing tcp-dump file
	FileName        = "./resources/data/dumps.tcp/parser.test.bin" // tcp-dump file with java protocol data
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Generate emulated profiler data and upload them to the appropriate buckets to S3 storage",
	Run:   Run,
}

func init() {
	rootCmd.AddCommand(runCmd)
	data.InitFlags(runCmd.Flags())
}

// Run
//
// High-level logic of data-generator
func Run(cmd *cobra.Command, args []string) {
	startTime := time.Now()
	ctx := cmd.Context()

	Cfg, err := data.PrepareConfig(ctx)
	if err != nil {
		os.Exit(1)
	}
	cfg := *Cfg

	log.Info(ctx, "Parsing original emulator data")
	var podData = parseJavaProtocolData(ctx, cfg)
	if cfg.OnlyParse {
		log.Info(ctx, "Argument `-parse` was used, only parse original data")
		return
	}

	if cfg.ClearPrevious {
		clearOutputDirs(ctx, cfg)
	}

	db, _ := pg.NewClient(ctx, cfg.Postgres) // could be null if no params (but fails if no connect)
	cloud := s3.New(ctx, cfg, db)            // could be null if no params (but fails if no connect)
	toolWorker := worker.New(ctx, cfg, db, cloud, podData)

	toolWorker.ProcessTemporary(ctx)
	toolWorker.ProcessHistorical(ctx)

	log.Info(ctx, "Done: in %v ", time.Since(startTime))
}

func clearOutputDirs(ctx context.Context, cfg data.Config) {
	clearDirectory(ctx, cfg.Out.OutputDir)
}

func clearDirectory(ctx context.Context, filepath string) {
	log.Info(ctx, "Clearing data from `%v` directory...", filepath)
	err := os.RemoveAll(filepath)
	if err != nil && !os.IsNotExist(err) {
		log.Fatal(ctx, err, "Could not remove directory `%v`", filepath)
	}
}

func parseJavaProtocolData(ctx context.Context, cfg data.Config) *parser.ParsedPodDump {
	startTime := time.Now()
	file := parser.TcpFile{FileName: FileName, FilePath: FileName}
	podData, err := parser.ParsePodTcpDump(ctx, file)
	if err != nil {
		panic(err)
	}

	pod := parser.ParsedPodDump{LoadedTcpData: podData}
	pod.ParseStreams(ctx, cfg.OnlyParse, cfg.Out.ParsedOutputDir)

	log.Info(ctx, "Parsed data for %v in %v", pod.PodName, time.Since(startTime))
	return &pod
}
