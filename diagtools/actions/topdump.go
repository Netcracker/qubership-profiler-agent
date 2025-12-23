package actions

import (
	"context"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
)

type JavaTopAction struct {
	Action
}

func CreateTopAction(ctx context.Context) (action JavaTopAction, err error) {
	action = JavaTopAction{}
	return action, nil
}

func (action *JavaTopAction) GetTop(ctx context.Context) (err error) {
	log.Infof(ctx, "topdump command is deprecated and has been disabled - skipping execution")
	return nil
}
