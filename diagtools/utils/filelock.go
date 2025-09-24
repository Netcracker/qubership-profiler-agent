package utils

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
)

const _lockPostfix = ".diagnostic.exclusivelock"

func Flock(filename string) (file *os.File, err error) {
	file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open file %s error: %v", filename, err)
	}

	_, err = file.WriteString(strconv.FormatInt(int64(os.Getpid()), 10))
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("write to file %s error: %v", filename, err)
	}

	err = flock(file, writeLock)
	if err != nil {
		_ = file.Close()
	}
	return
}

func Unlock(file *os.File) (err error) {
	err = unlock(file)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		clErr := file.Close()
		if clErr != nil {
			err = fmt.Errorf("file close error: %v", clErr)
		}
	}(file)

	return nil
}

func GetLockFileName() string {
	diagFolder := constants.DiagFolder()
	return filepath.Join(diagFolder, _lockPostfix)
}

func InLock(ctx context.Context, f func(context.Context) error) error {
	file, e := Flock(GetLockFileName())
	if e != nil {
		log.Error(ctx, e, "Can't handle request, problem with lock file")
		return e
	}

	err := f(ctx)

	e = Unlock(file)
	if e != nil {
		log.Error(ctx, e, "file unlock failed")
	}
	if err == nil && e != nil { // clean unlock should not override error in main function
		err = e
	}

	return err
}

func (lt lockType) String() string {
	switch lt {
	case readLock:
		return "RLock"
	case writeLock:
		return "Lock"
	default:
		return "Unlock"
	}
}
