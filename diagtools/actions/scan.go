package actions

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/utils"
)

var (
	reDumpFileName = regexp.MustCompile("([0-9]+T[0-9]+).hprof$")
)

type ScanAction struct {
	Action
	FilesToSend []string
}

func CreateScanAction(ctx context.Context) (action ScanAction, err error) {
	action = ScanAction{
		Action: Action{
			DcdEnabled: constants.IsDcdEnabled(),
			DumpPath:   constants.DumpFolder(),
			CmdTimeout: 10 * time.Second,
		},
	}
	err = action.GetPodName(ctx)
	return action, err
}

func (action *ScanAction) scanFiles(ctx context.Context, args []string) (fileNames []string, err error) {
	for _, pathPattern := range args {
		var files []string
		if files, err = filepath.Glob(filepath.Clean(pathPattern)); err != nil {
			return nil, err
		}
		if len(files) > 0 {
			log.Debugf(ctx, "found %d files for pattern '%s': %v", len(files), pathPattern, files)
			fileNames = append(fileNames, files...)
		}
	}
	log.Infof(ctx, "files to be processed: %s", fileNames)
	return fileNames, nil
}

func (action *ScanAction) RunScan(ctx context.Context, args []string) (err error) {
	var fileNames []string
	fileNames, err = action.scanFiles(ctx, args)
	if err != nil {
		return err
	}

	for i, filename := range fileNames {
		fileCtx := log.AppendCtx(ctx, fmt.Sprintf("zip#%d", i))
		if !strings.HasSuffix(filename, constants.DumpFileSuffix) {
			action.FilesToSend = append(action.FilesToSend, filename)
			continue
		}

		if !reDumpFileName.MatchString(filename) {
			action.ZipDumpPath = action.prepareFileName(filename)
		}
		action.DumpPath = filename
		err = action.ZipDump(fileCtx)
		if err != nil {
			return
		}

		action.FilesToSend = append(action.FilesToSend, action.ZipDumpPath)
	}

	if action.DcdEnabled && len(action.FilesToSend) > 0 {
		for i, filePath := range action.FilesToSend {
			fileCtx := log.AppendCtx(ctx, fmt.Sprintf("send#%d", i))
			action.ZipDumpPath = filePath

			err = action.GetTargetUrl(fileCtx)
			if err != nil {
				return
			}

			// uploadToDiagnosticCenter
			err = utils.SendSingleFile(fileCtx, action.TargetUrl, filePath)
			//err = utils.SendMultiPart(ctx, action.TargetUrl, filePath)
			if err != nil {
				return
			}

			err = os.Remove(filePath)
			if err != nil {
				log.Error(fileCtx, err, "failed to delete file", "name", filePath)
				return
			}
		}
	}

	return
}

func (action *ScanAction) prepareFileName(filename string) string {
	var builder strings.Builder
	filenameTS := time.Now().UTC().Format(constants.DumpFileTimestampLayout)
	builder.WriteString(filepath.Dir(filename))
	builder.WriteString(string(filepath.Separator))
	builder.WriteString(filenameTS)
	builder.WriteString(constants.DumpFileSuffix)
	builder.WriteString(".zip")
	return builder.String()
}
