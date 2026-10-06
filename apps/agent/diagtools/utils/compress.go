package utils

import (
	"archive/zip"
	"compress/flate"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
)

func GetCompressionLevel(ctx context.Context) int {
	env := constants.ZipCompressionLevel

	if val, exist := os.LookupEnv(env); exist {
		if compressionLevel, err := strconv.Atoi(val); err != nil {
			return compressionLevel
		} else {
			log.Errorf(ctx, err, "Parsing %s failed. Default compression level will be used", env)
		}
	} else {
		log.Infof(ctx, "%s is not defined. Default compression level will be used", env)
	}
	return flate.DefaultCompression
}

func ZipDump(ctx context.Context, originFile, originZipPath string) (zipPath string, err error) {
	startTime := time.Now()

	if len(originZipPath) == 0 {
		var builder strings.Builder
		builder.WriteString(originFile)
		builder.WriteString(".zip")
		zipPath = builder.String()
	} else {
		zipPath = originZipPath
	}

	var originSize int64
	originSize, err = FileSize(ctx, originFile)
	if err != nil {
		return zipPath, err
	} else {
		log.Infof(ctx, "found '%s', size in bytes: %d", originFile, originSize)
	}

	compressionLevel := GetCompressionLevel(ctx)
	log.Infof(ctx, "compressing '%s' to '%s'", originFile, zipPath)
	err = zipDump(ctx, compressionLevel, originFile, zipPath)

	if err == nil {

		if zipSize, e := FileSize(ctx, zipPath); e != nil {
			return zipPath, e
		} else {
			latency := time.Since(startTime)
			rate := fmt.Sprintf("%.1f mb/s", float64(zipSize)/latency.Seconds()/1024/1024)
			if latency < time.Second {
				rate = "?"
			}
			percent := "?"
			if originSize > 0 {
				percent = fmt.Sprintf("%d%%", zipSize*100/originSize)
			}
			log.Info(ctx,
				fmt.Sprintf("Compressed %s to %s", originFile, zipPath),
				"zip", zipSize, "origin", originSize, "rate", rate, "p", percent, "duration", latency)
		}

		err = os.Remove(originFile)
		if err != nil {
			log.Error(ctx, err, "failed to delete original file", "name", originFile)
			return
		}

	}
	return zipPath, err
}

func zipDump(ctx context.Context, compressionLevel int, originFile, zipPath string) (err error) {
	var zipArchive *os.File
	zipArchive, err = os.Create(zipPath)
	if err != nil {
		log.Error(ctx, err, "failed to create zip file", "zip", zipPath)
		return
	}
	defer func(zipArchive *os.File) {
		err = zipArchive.Close()
		if err != nil {
			log.Error(ctx, err, "failed to close zip file", "zip", zipPath)
		}
	}(zipArchive)

	var hFile *os.File
	hFile, err = os.Open(originFile)
	if err != nil {
		log.Error(ctx, err, "failed to open file", "file", originFile)
		return
	}
	defer func(hFile *os.File) {
		errClose := hFile.Close()
		if errClose != nil {
			log.Error(ctx, errClose, "failed to close file", "file", originFile)
			err = errClose
			return
		}
	}(hFile)

	zipWriter := zip.NewWriter(zipArchive)
	defer func(zipWriter *zip.Writer) {
		errClose := zipWriter.Close()
		if errClose != nil {
			err = errClose
			log.Error(ctx, err, "failed to finish the zip compress process")
		}
	}(zipWriter)

	// Register a Deflate compressor with the specified compression level.
	zipWriter.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(out, compressionLevel)
	})

	var writer io.Writer
	writer, err = zipWriter.Create(filepath.Base(originFile))
	if err != nil {
		log.Error(ctx, err, "failed to add file to the zip file", "file", originFile)
		return
	}
	_, err = io.Copy(writer, hFile)
	if err != nil {
		log.Error(ctx, err, "failed to copy file content to the zip file", "zip", zipPath)
	}

	return
}
