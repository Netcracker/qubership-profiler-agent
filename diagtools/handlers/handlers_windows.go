//go:build windows

package handlers

import (
	"context"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
)

func HandleTopCmd(ctx context.Context) (err error) {
	log.Info(ctx, "'top' command is not supported under 'windows'")
	return
}
