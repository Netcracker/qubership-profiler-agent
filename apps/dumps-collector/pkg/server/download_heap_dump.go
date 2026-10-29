package server

import (
	"archive/zip"
	"mime"
	"os"
	"path/filepath"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/task"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/labstack/echo/v5"
)

func downloadHeapDump(requestProcessor *task.RequestProcessor) func(c *echo.Context) error {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()

		// Get parameters from query params
		handle, apiError := getPathParam(c, "handle")
		if apiError != nil {
			return apiError.ReturnWithError(c)
		}

		fileLocation, err := requestProcessor.HeapDumpDownloadFile(ctx, handle)
		if err != nil {
			return NewAPIError(500, err).ReturnWithError(c)
		}

		if fileLocation.PathToZip == "" {
			path := fileLocation.PathToFiles[0]
			name := filepath.Base(path)
			c.Response().Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
			return c.FileFS(name, os.DirFS(filepath.Dir(path)))
		} else {
			zipReader, err := zip.OpenReader(fileLocation.PathToZip)
			if err != nil {
				log.Error(ctx, err, "Error opening zip %s to collect dumps", fileLocation.PathToZip)
				return NewAPIError(500, err).ReturnWithError(c)
			}
			defer func() { _ = zipReader.Close() }()

			// Separator replacement is needed for windows run, because Open requires '/' separator
			f, err := zipReader.Open(filepath.ToSlash(fileLocation.PathToFiles[0]))
			if err != nil {
				log.Error(ctx, err, "Error opening file %s in zip %s", fileLocation.PathToFiles[0], fileLocation.PathToZip)
				return NewAPIError(500, err).ReturnWithError(c)
			}
			defer func() { _ = f.Close() }()

			fileName := filepath.Base(fileLocation.PathToFiles[0])
			c.Response().Header().Set("Content-Disposition", "attachment; filename="+fileName+";")
			return c.Stream(200, "application/zip", f)
		}
	}
}
