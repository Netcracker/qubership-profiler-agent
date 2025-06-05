//go:build windows

package handlers

import (
	"context"
	"qubership-agent/diagtools/log"
)

func HandleTopCmd(ctx context.Context) (err error) {
	log.Info(ctx, "'top' command is not supported under 'windows'")
	return
}
