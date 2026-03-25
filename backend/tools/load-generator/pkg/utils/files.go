package utils

import (
	"context"
	"fmt"
	"os"
)

// CheckDir make sure that directory is exists
func CheckDir(ctx context.Context, dirName string) error {
	err := os.MkdirAll(dirName, 0700)
	if err != nil && !os.IsExist(err) {
		LogError(ctx, err, "Can't create directory '%v'", dirName)
		return err
	}
	return nil
}

// FilesList reads the named directory, returning all its directory entries sorted by filename
func FilesList(ctx context.Context, dirName string) (files []os.DirEntry, err error) {
	err = CheckDir(ctx, dirName)

	if err == nil {
		files, err = os.ReadDir(dirName)
	}
	if err != nil {
		LogError(ctx, err, "Failed to read directory '%s'", dirName)
		err = fmt.Errorf("failed to read directory %s: %s", dirName, err.Error())
	}
	return files, err
}

func FileSize(ctx context.Context, filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		LogError(ctx, err, "could open file '%s'", filePath)
		return 0, err
	}
	defer file.Close()

	objectStat, err := file.Stat()
	if err != nil {
		LogError(ctx, err, "could get stat for file '%s'", filePath)
		return 0, err
	}

	return objectStat.Size(), nil
}

func ClearDirectory(ctx context.Context, filepath string) {
	LogInfo(ctx, "Clearing data from `%v` directory...", filepath)
	err := os.RemoveAll(filepath)
	if err != nil && !os.IsNotExist(err) {
		LogFatal(ctx, err, "Could not remove directory `%v`", filepath)
	}
}
