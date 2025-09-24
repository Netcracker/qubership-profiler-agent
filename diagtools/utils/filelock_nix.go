//go:build !windows

package utils

import (
	"io/fs"
	"os"
	"syscall"
)

type lockType int16

const (
	readLock  lockType = syscall.LOCK_SH
	writeLock lockType = syscall.LOCK_EX
)

func flock(file *os.File, lt lockType) (err error) {
	for {
		err = syscall.Flock(int(file.Fd()), int(lt))
		if err != syscall.EINTR {
			break
		}
	}
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
	return flock(file, syscall.LOCK_UN)
}
