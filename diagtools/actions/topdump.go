package actions

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
)

type JavaTopAction struct {
	Action
}

func CreateTopAction(ctx context.Context) (action JavaTopAction, err error) {
	action = JavaTopAction{
		Action: Action{
			DcdEnabled: constants.IsDcdEnabled(),
			DumpPath:   constants.DumpFolder(),
			PidName:    "java",
			Command:    "top",
			CmdArgs:    []string{"-Hb", "-p{{.Pid}}", "-oTIME+", "-d60", "-n1"},
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

func (action *JavaTopAction) GetTop(ctx context.Context) (err error) {
	err = action.GetDumpFile(constants.TopDumpSuffix)
	if err != nil {
		return
	}

	err = action.GetParams()
	if err != nil {
		return
	}

	log.Infof(ctx, "collecting CPU usage for PID #%v to %s", action.Pid, action.DumpPath)
	var output []byte
	output, err = action.RunJCmdWithOutput(ctx)
	if err != nil {
		return
	}
	log.Infof(ctx, "top dump taken, size in bytes: %d", len(output))

	if action.DcdEnabled && len(output) > 0 {
		err = action.GetTargetUrl(ctx)
		if err == nil {
			err = action.UploadOutputToDiagnosticCenter(ctx, output)
		}
	}
	return
}
