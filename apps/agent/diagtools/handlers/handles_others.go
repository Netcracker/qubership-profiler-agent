//go:build !windows

package handlers

import (
	"context"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/actions"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
)

func HandleTopCmd(ctx context.Context) (err error) {
	action, err := actions.CreateTopAction(ctx)
	if err != nil {
		return
	}

	log.Infof(ctx, "start collecting CPU usage for PID: %s", action.Pid)
	err = action.GetTop(ctx)
	return
}
