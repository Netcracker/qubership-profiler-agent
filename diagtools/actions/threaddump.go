package actions

import (
	"context"
	"strconv"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
	"github.com/vlsi/jattach/v2"
)

type JavaThreadDumpAction struct {
	Action
}

func CreateThreadDumpAction(ctx context.Context) (action JavaThreadDumpAction, err error) {
	action = JavaThreadDumpAction{
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

func (action *JavaThreadDumpAction) GetThreadDump(ctx context.Context) (err error) {
	err = action.GetDumpFile(constants.ThreadDumpSuffix)
	if err != nil {
		return
	}

	log.Infof(ctx, "collecting thread dump from JAVA_PID #%v to %s", action.Pid, action.DumpPath)

	// Convert PID string to int for jattach
	var pid int
	pid, err = strconv.Atoi(action.Pid)
	if err != nil {
		log.Errorf(ctx, err, "failed to parse PID: %s", action.Pid)
		return
	}

	// Use jattach to get thread dump
	var outputStr string
	outputStr, err = jattach.GetThreadDump(pid)
	if err != nil {
		log.Errorf(ctx, err, "failed to get thread dump using jattach")
		return
	}

	output := []byte(outputStr)
	log.Infof(ctx, "thread dump taken, size in bytes: %d", len(output))

	if action.DcdEnabled && len(output) > 0 {
		err = action.GetTargetUrl(ctx)
		if err == nil {
			err = action.UploadOutputToDiagnosticCenter(ctx, output)
		}
	}
	return
}
