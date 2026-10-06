package files

import (
	"context"
	"fmt"
	"os"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"gopkg.in/yaml.v3"
)

// CheckDir make sure that directory is exists
func CheckDir(ctx context.Context, dirName string) error {
	err := os.MkdirAll(dirName, 0700)
	if err != nil && !os.IsExist(err) {
		log.Error(ctx, err, "Can't create directory '%v'", dirName)
		return err
	}
	return nil
}

// List reads the named directory, returning all its directory entries sorted by filename
func List(ctx context.Context, dirName string) (files []os.DirEntry, err error) {
	err = CheckDir(ctx, dirName)

	if err == nil {
		files, err = os.ReadDir(dirName)
	}
	if err != nil {
		log.Error(ctx, err, "Failed to read directory '%s'", dirName)
		err = fmt.Errorf("failed to read directory %s: %s", dirName, err.Error())
	}
	return files, err
}

// FileSize size of file in bytes
func FileSize(ctx context.Context, filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Error(ctx, err, "could open file '%s'", filePath)
		return 0, err
	}
	defer file.Close()

	objectStat, err := file.Stat()
	if err != nil {
		log.Error(ctx, err, "could get stat for file '%s'", filePath)
		return 0, err
	}

	return objectStat.Size(), nil
}

// ClearDirectory clear files in directory if exists
func ClearDirectory(ctx context.Context, filepath string) error {
	log.Debug(ctx, "Clearing data from `%v` directory...", filepath)
	err := os.RemoveAll(filepath)
	if err != nil && !os.IsNotExist(err) {
		log.Error(ctx, err, "Could not remove directory `%v`", filepath)
	} else {
		err = nil // ignore os.IsNotExist
	}
	return err
}

// CheckFile is used to check, if file is defined and exists
func CheckFile(filePath string) error {
	if filePath == "" {
		return fmt.Errorf("file is undefined")
	}
	info, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("can't get info for %s: %s", filePath, err)
	}
	if info.IsDir() {
		return fmt.Errorf("%s is not a file", filePath)
	}
	return nil
}

// ParseYamlFile is used to parse given file to specified object
func ParseYamlFile[T interface{}](filePath string, obj T) error {
	if err := CheckFile(filePath); err != nil {
		return err
	}
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0600)
	if err != nil {
		return fmt.Errorf("can't read file %s: %s", filePath, err)
	}

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	err = decoder.Decode(obj)
	if err != nil {
		return fmt.Errorf("can't parse file content %s: %s", filePath, err)
	}
	return nil
}
