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

func HandleGcLogCmd(ctx context.Context, action *actions2.GcLogAction) (err error) {
	if !constants.IsGcLogEnabled(ctx) {
		log.Debug(ctx, "GC log collection is disabled")
		return
	}

	log.Info(ctx, "start collecting GC logs")
	err = action.CollectGcLogs(ctx)
	return
}

func HandleScanCmd(ctx context.Context, args []string) (err error) {
	action, err := createAndZipScan(ctx, args)
	if err != nil {
		return
	}
	return action.UploadFiles(ctx)
}

// createAndZipScan finds dump files and compresses any raw .hprof into .zip.
// The returned action has FilesToSend populated but nothing uploaded yet.
//
// Before scanning, enforces retention on the same patterns (max age and
// combined-size quota) so that crash artifacts the collector never accepted
// don't pile up forever on disk — see DIAGNOSTIC_UPLOAD_MAX_AGE and
// DIAGNOSTIC_PENDING_MAX_BYTES. Applies to every scan path, including
// ad-hoc `diagtools scan *.hprof* ./core* ./hs_err*` and the scheduled scan.
func createAndZipScan(ctx context.Context, args []string) (action actions2.ScanAction, err error) {
	if len(args) == 0 {
		err = fmt.Errorf("no scan patterns")
		log.Error(ctx, err, "there are no file patterns as arguments")
		return
	}

	actions2.CleanupStaleDumps(
		ctx,
		args,
		constants.UploadMaxAge(ctx),
		constants.PendingMaxBytes(ctx),
	)

	action, err = actions2.CreateScanAction(ctx)
	if err != nil {
		return
	}

	startTime := time.Now()
	log.Info(ctx, "start to scan files")
	err = action.ZipScannedFiles(ctx, args)
	log.Info(ctx, "scanning is done",
		"files", len(action.FilesToSend), "duration", time.Since(startTime))
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

	gcLogAction, gcLogErr := actions2.CreateGcLogAction(baseCtx)
	if gcLogErr != nil {
		log.Error(baseCtx, gcLogErr, "failed to create GC log action, GC log collection will be skipped")
	}

	// Exponential backoff for scan upload failures.
	// Zip phase always runs (to compress new .hprof files promptly).
	// Upload phase is skipped while in backoff.
	// On upload failure: double the delay (capped at 32x scanInterval).
	// On upload success: reset.
	var uploadResumeAt time.Time
	uploadBackoff := time.Duration(0)
	maxUploadBackoff := 32 * scanInterval

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
			var uploadErr error
			err = utils.InLock(ctx, func(ctx context.Context) error {
				dumpFolder := constants.DumpFolder()
				filePattern := filepath.Join(dumpFolder, constants.DumpFilePattern)

				action, zipErr := createAndZipScan(ctx, []string{filePattern})
				if zipErr != nil {
					log.Error(ctx, zipErr, "Scan zip failed")
					return zipErr
				}

				if now := time.Now(); now.Before(uploadResumeAt) {
					log.Infof(ctx, "Upload skipped due to backoff, next attempt in %s",
						uploadResumeAt.Sub(now).Truncate(time.Second))
					return nil
				}

				uploadErr = action.UploadFiles(ctx)
				if uploadErr != nil {
					log.Error(ctx, uploadErr, "Scan upload failed")
				} else {
					log.Info(ctx, "Scan request done")
				}

				if gcLogAction != nil {
					gcErr := HandleGcLogCmd(ctx, gcLogAction)
					if gcErr != nil {
						log.Error(ctx, gcErr, "GC log collection failed")
						err = errors.Join(err, gcErr)
					}
				}

				return uploadErr
			})
			if uploadErr != nil {
				if uploadBackoff == 0 {
					uploadBackoff = scanInterval
				} else {
					uploadBackoff *= 2
				}
				if uploadBackoff > maxUploadBackoff {
					uploadBackoff = maxUploadBackoff
				}
				uploadResumeAt = time.Now().Add(uploadBackoff)
				log.Infof(ctx, "Upload failed, backing off for %s", uploadBackoff)
			} else if uploadResumeAt.IsZero() || time.Now().After(uploadResumeAt) {
				// Reset only when upload actually ran (not when skipped by backoff)
				uploadBackoff = 0
				uploadResumeAt = time.Time{}
			}
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
