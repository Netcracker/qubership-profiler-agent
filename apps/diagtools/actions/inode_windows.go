//go:build windows

package actions

import "os"

type inodeID uint64

// getInode is not supported on Windows. Returns 0, which means
// every stat comparison will look like a rotation, causing a full re-upload.
// This is safe but less efficient. Diagtools primarily targets Linux containers.
func getInode(info os.FileInfo) inodeID {
	return 0
}
