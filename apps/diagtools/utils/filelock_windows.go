//go:build windows

package utils

import (
	"io/fs"
	"os"

	"golang.org/x/sys/windows"
)

type lockType uint32

const (
	readLock  lockType = 0
	writeLock lockType = windows.LOCKFILE_EXCLUSIVE_LOCK
)

const (
	reserved = 0
	allBytes = ^uint32(0)
)

func flock(file *os.File, lt lockType) error {
	// Per https://golang.org/issue/19098, “Programs currently expect the Fd
	// method to return a handle that uses ordinary synchronous I/O.”
	// However, LockFileEx still requires an OVERLAPPED structure,
	// which contains the file offset of the beginning of the lock range.
	// We want to lock the entire file, so we leave the offset as zero.
	ol := new(windows.Overlapped)

	err := windows.LockFileEx(windows.Handle(file.Fd()), uint32(lt), reserved, allBytes, allBytes, ol)
	if err != nil {
		return &fs.PathError{
			Op:   lt.String(),
			Path: file.Name(),
			Err:  err,
		}
	}
	return nil
}

func unlock(file *os.File) error {
	ol := new(windows.Overlapped)
	err := windows.UnlockFileEx(windows.Handle(file.Fd()), reserved, allBytes, allBytes, ol)
	if err != nil {
		return &fs.PathError{
			Op:   "Unlock",
			Path: file.Name(),
			Err:  err,
		}
	}
	return nil
}
