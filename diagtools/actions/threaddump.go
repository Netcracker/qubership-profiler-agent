package actions

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
)

type JavaThreadDumpAction struct {
	Action
}

func CreateThreadDumpAction(ctx context.Context) (action JavaThreadDumpAction, err error) {
	longListing := constants.LongListeningArgs()
	args := []string{"{{.Pid}}"}
	if len(longListing) > 0 {
		args = []string{longListing, "{{.Pid}}"}
	}

	action = JavaThreadDumpAction{
		Action: Action{
			DcdEnabled: constants.IsDcdEnabled(),
			DumpPath:   constants.DumpFolder(),
			PidName:    "java",
			Command:    "jstack",
			CmdArgs:    args,
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

	err = action.GetParams()
	if err != nil {
		return
	}

	log.Infof(ctx, "collecting thread dump from JAVA_PID #%v to %s", action.Pid, action.DumpPath)
	var output []byte
	output, err = action.RunJCmdWithOutput(ctx)
	if err != nil {
		return
	}
	log.Infof(ctx, "thread dump taken, size in bytes: %d", len(output))

	if action.DcdEnabled && len(output) > 0 {
		err = action.GetTargetUrl(ctx)
		if err == nil {
			err = action.UploadOutputToDiagnosticCenter(ctx, output)
		}
	}
	return
}
