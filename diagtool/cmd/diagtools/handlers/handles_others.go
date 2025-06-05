//go:build !windows

package handlers

import (
	"context"
	"qubership-agent/diagtools/actions"
	"qubership-agent/diagtools/log"
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
