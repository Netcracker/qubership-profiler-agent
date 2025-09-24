package handlers

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	actions2 "github.com/Netcracker/qubership-profiler-agent/diagtools/actions"
	config2 "github.com/Netcracker/qubership-profiler-agent/diagtools/config"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/utils"
)

type Type string

const (
	HeapModifier         Type = "heap"
	DumpModifier         Type = "dump"
	ScanModifier         Type = "scan"
	ScheduleModifier     Type = "schedule"
	ZkConfigModifier     Type = "zkcfg"
	ConsulConfigModifier Type = "consulcfg"
	ConfigServerModifier Type = "servercfg"
)

func (ct Type) String() string {
	return string(ct)
}

func AsType(str string) Type {
	lowerStr := Type(strings.ToLower(str))
	switch lowerStr {
	case HeapModifier, DumpModifier, ScanModifier, ScheduleModifier,
		ZkConfigModifier, ConsulConfigModifier, ConfigServerModifier:
		return lowerStr
	default:
		return ""
	}
}

func HandleHeapDumpCmd(ctx context.Context, args []string) (err error) {
	action, err := actions2.CreateHeapDumpAction(ctx)
	if err != nil {
		return
	}

	log.Infof(ctx, "start creating a heap dump for PID #%s", action.Pid)
	// cmd: heap zip upload
	heapDumpZip := slices.Contains(args, "zip")
	heapDumpUpload := slices.Contains(args, "upload")

	err = action.GetHeapDump(ctx, heapDumpZip, heapDumpUpload)
	return
}

func HandleDumpCmd(ctx context.Context) (err error) {
	if constants.IsThreadDumpEnabled(ctx) {
		err = errors.Join(err, HandleThreadDumpCmd(ctx))
	}

	if constants.IsTopDumpEnabled(ctx) {
		err = errors.Join(err, HandleTopCmd(ctx))
	}

	return
}

func HandleThreadDumpCmd(ctx context.Context) (err error) {
	action, err := actions2.CreateThreadDumpAction(ctx)
	if err == nil {
		log.Infof(ctx, "start creating a thread dump for PID #%s", action.Pid)
		err = action.GetThreadDump(ctx)
	}
	return
}

func HandleScanCmd(ctx context.Context, args []string) (err error) {
	// scan "${NC_DIAGNOSTIC_LOG_FOLDER}"/*.hprof* ./core* ./hs_err*
	if len(args) == 0 {
		log.Error(ctx, fmt.Errorf("no scan patterns"), "there are no file patterns as arguments")
		return
	}

	action, err := actions2.CreateScanAction(ctx)
	if err == nil {
		startTime := time.Now()
		log.Info(ctx, "start to scan files")
		err = action.RunScan(ctx, args)
		log.Info(ctx, "scanning is done",
			"files", len(action.FilesToSend), "duration", time.Since(startTime))
	}
	return
}

func HandleScheduleCmd(baseCtx context.Context, logPath string) (err error) {
	dumpInterval := constants.DumpInterval(baseCtx)
	dumpIntervalTicker := time.NewTicker(dumpInterval)
	defer dumpIntervalTicker.Stop()

	scanInterval := constants.ScanInterval(baseCtx)
	scanIntervalTicker := time.NewTicker(scanInterval)
	defer scanIntervalTicker.Stop()

	logIntervalDays := constants.LogInterval()
	logInterval := 24 * time.Hour * time.Duration(logIntervalDays)
	logIntervalTicker := time.NewTicker(logInterval)
	defer logIntervalTicker.Stop()

	for {
		select {
		case <-dumpIntervalTicker.C:
			ctx := log.ChildCtx(baseCtx, "schedule:dump")
			log.Info(ctx, "Dump request")
			err = utils.InLock(ctx, func(ctx context.Context) error {
				err = HandleDumpCmd(ctx)
				if err != nil {
					log.Error(ctx, err, "Dump request failed")
				} else {
					log.Info(ctx, "Dump request done")
				}
				return err
			})
		case <-scanIntervalTicker.C:
			ctx := log.ChildCtx(baseCtx, "schedule:scan")
			log.Info(ctx, "Scan request")
			err = utils.InLock(ctx, func(ctx context.Context) error {
				dumpFolder := constants.DumpFolder()
				filePattern := filepath.Join(dumpFolder, constants.DumpFilePattern)

				err = HandleScanCmd(ctx, []string{filePattern})
				if err != nil {
					log.Error(ctx, err, "Scan request failed")
				} else {
					log.Info(ctx, "Scan request done")
				}
				return err
			})
		case <-logIntervalTicker.C:
			ctx := log.ChildCtx(baseCtx, "schedule:clean")
			log.Info(ctx, "Clean log request")
			err = HandleCleanLogsCmd(ctx, logPath, logInterval)
			if err != nil {
				log.Error(ctx, err, "Clean log request failed")
			} else {
				log.Info(ctx, "Clean log request done")
			}
		case <-baseCtx.Done():
			log.Info(baseCtx, "Forced stopping request")
			return
		}
	}
}

func HandleCleanLogsCmd(ctx context.Context, logPath string, logInterval time.Duration) (err error) {
	log.Infof(ctx, "Cleaning diagnostic logs older than %s", logInterval)

	if constants.IsDcdEnabled() {
		var dEntry []os.DirEntry
		dEntry, err = os.ReadDir(logPath)
		if err != nil {
			return
		}

		for _, de := range dEntry {
			if de.IsDir() || !strings.HasSuffix(de.Name(), ".log") ||
				(strings.Contains(de.Name(), ScheduleModifier.String()) && strings.HasSuffix(de.Name(), ".log")) {
				continue
			}

			fullPath := filepath.Join(logPath, de.Name())

			var info fs.FileInfo
			info, err = de.Info()
			if err != nil {
				log.Errorf(ctx, err, "Failed to get file %s info", fullPath)
			}

			diff := time.Since(info.ModTime())
			if diff >= logInterval {
				log.Infof(ctx, "Deleting %s which is %s old", info.Name(), diff)
				err = os.Remove(fullPath)
				if err != nil {
					log.Errorf(ctx, err, "Failed to remove file %s", fullPath)
				}
			}
		}
	}

	return
}

func HandleZkConfigCmd(ctx context.Context, args []string) (err error) {
	if !constants.IsZookeeperEnabled() {
		log.Info(ctx, "Zookeeper integration is not enabled")
		return
	}

	zkCfg := config2.ZkCfg{}
	// zkCfg "${NC_DIAGNOSTIC_FOLDER}/properties" esc.config NC_DIAGNOSTIC_ESC_ENABLED ...
	err = zkCfg.Prepare(args)
	if err != nil {
		return
	}

	err = zkCfg.ExportConfig(ctx)
	if err != nil {
		return
	}

	if errProps := zkCfg.FilterProperties(ctx); errProps != nil {
		err = errors.Join(err, errProps)
	}

	err = errors.Join(err, zkCfg.CheckEscConfigFile(ctx, constants.ZkCustomConfig))

	return
}

func HandleConsulConfigCmd(ctx context.Context, args []string) (err error) {
	// Trying to get general consul environment variables
	if !constants.IsConsulEnabled() {
		log.Info(ctx, "Consul integration is not enabled")
		return
	}

	consulCfg := config2.ConsulCfg{}
	// consulCfg "${NC_DIAGNOSTIC_FOLDER}/properties" esc.config NC_DIAGNOSTIC_ESC_ENABLED ...
	err = consulCfg.Prepare(args)
	if err != nil {
		return
	}

	err = consulCfg.ExportConfig(ctx)
	if err != nil {
		return
	}

	if errProps := consulCfg.FilterProperties(ctx); errProps != nil {
		err = errors.Join(err, errProps)
	}

	err = errors.Join(err, consulCfg.CheckEscConfigFile(ctx, constants.ZkCustomConfig))

	return
}

func HandleConfigServerCmd(ctx context.Context, args []string) (err error) {
	if !constants.IsConfigServerEnabled() {
		log.Infof(ctx, "ConfigServer integration is not enabled, %s is empty", constants.ConfigServerAddress)
		return
	}

	configServerCfg := config2.ServerCfg{}
	// serverCfg "${NC_DIAGNOSTIC_FOLDER}/properties" esc.config NC_DIAGNOSTIC_ESC_ENABLED ...
	err = configServerCfg.Prepare(args)
	if err != nil {
		return
	}

	err = configServerCfg.ExportConfig(ctx)
	if err != nil {
		return
	}
	if errProps := configServerCfg.FilterProperties(ctx); errProps != nil {
		err = errors.Join(err, errProps)
	}

	err = errors.Join(err, configServerCfg.CheckEscConfigFile(ctx, constants.ZkCustomConfig))

	return
}
