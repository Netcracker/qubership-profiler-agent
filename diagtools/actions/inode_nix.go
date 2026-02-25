//go:build !windows

package actions

import (
	"os"
	"syscall"
)

type inodeID uint64

func getInode(info os.FileInfo) inodeID {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}
	return inodeID(stat.Ino)
}
