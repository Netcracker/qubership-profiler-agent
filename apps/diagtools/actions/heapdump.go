package actions

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/utils"
	"github.com/vlsi/jattach/v2"
)

type JavaHeapDumpAction struct {
	Action
}

func CreateHeapDumpAction(ctx context.Context) (action JavaHeapDumpAction, err error) {
	action = JavaHeapDumpAction{
		Action: Action{
			DcdEnabled: constants.IsDcdEnabled(),
			DumpPath:   constants.DumpFolder(),
			PidName:    "java",
			CmdTimeout: 10 * time.Second,
		},
	}

	err = action.GetPodName(ctx)
	if err != nil {
		return
	}
	action.Pid, err = action.GetPid(ctx)
	if err != nil {
		return
	}

	return action, nil
}

func (action *JavaHeapDumpAction) GetHeapDump(ctx context.Context, heapDumpZip, heapDumpUpload bool) (err error) {
	err = action.GetDumpFile(constants.DumpFileSuffix)
	if err != nil {
		return
	}

	log.Infof(ctx, "collecting heap dump from JAVA_PID #%v to %s", action.Pid, action.DumpPath)

	// Convert PID string to int for jattach
	var pid int
	pid, err = strconv.Atoi(action.Pid)
	if err != nil {
		log.Errorf(ctx, err, "failed to parse PID: %s", action.Pid)
		return
	}

	// Use jattach to get heap dump
	err = jattach.HeapDump(pid, action.DumpPath)
	if err != nil {
		log.Errorf(ctx, err, "failed to get heap dump using jattach")
		return
	}

	if fSize, e := utils.FileSize(ctx, action.DumpPath); e != nil {
		return e
	} else {
		log.Infof(ctx, "heap dump taken, size in bytes: %d", fSize)
	}

	if heapDumpZip {
		err = action.ZipDump(ctx)
		if err != nil {
			return
		}

		if heapDumpUpload && action.DcdEnabled {
			err = action.GetTargetUrl(ctx)
			if err != nil {
				return
			}

			// uploadToDiagnosticCenter
			err = utils.SendMultiPart(ctx, action.TargetUrl, action.ZipDumpPath)
			if err != nil {
				return
			}

			err = os.Remove(action.ZipDumpPath)
			if err != nil {
				log.Error(ctx, err, "failed to delete file", "name", action.ZipDumpPath)
			}
		}
	}

	return
}
