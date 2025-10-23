package server

import (
	"archive/zip"
	"path/filepath"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/task"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/labstack/echo/v4"
)

func downloadHeapDump(requestProcessor *task.RequestProcessor) func(c echo.Context) error {
	return func(c echo.Context) error {
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
			return c.Attachment(fileLocation.PathToFiles[0], filepath.Base(fileLocation.PathToFiles[0]))
		} else {
			zipReader, err := zip.OpenReader(fileLocation.PathToZip)
			if err != nil {
				log.Error(ctx, err, "Error openning zip %s to collect dumps", fileLocation.PathToZip)
				return NewAPIError(500, err).ReturnWithError(c)
			}
			defer zipReader.Close()

			// Separator replacement is needed for windows run, because Open requires '/' separator
			f, err := zipReader.Open(filepath.ToSlash(fileLocation.PathToFiles[0]))
			if err != nil {
				log.Error(ctx, err, "Error opening file %s in zip %s", fileLocation.PathToFiles[0], fileLocation.PathToZip)
				return NewAPIError(500, err).ReturnWithError(c)
			}
			defer f.Close()

			fileName := filepath.Base(fileLocation.PathToFiles[0])
			c.Response().Header().Set("Content-Disposition", "attachment; filename="+fileName+";")
			return c.Stream(200, "application/zip", f)
		}
	}
}
