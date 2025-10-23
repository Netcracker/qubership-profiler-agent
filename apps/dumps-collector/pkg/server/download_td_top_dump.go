package server

import (
	"archive/zip"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/task"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/labstack/echo/v4"
)

func downloadTdTopDump(requestProcessor *task.RequestProcessor) func(c echo.Context) error {
	return func(c echo.Context) error {
		ctx := c.Request().Context()

		// Get parameters from query params
		dateFromMSec, apiError := getQueryInt64(c, "dateFrom")
		if apiError != nil {
			return apiError.ReturnWithError(c)
		}
		dateToMSec, apiError := getQueryInt64(c, "dateTo")
		if apiError != nil {
			return apiError.ReturnWithError(c)
		}
		dumpType, apiError := getQueryDumpType(c, "type")
		if apiError != nil {
			return apiError.ReturnWithError(c)
		}

		namespace, apiError := getQueryString(c, "namespace")
		if apiError != nil {
			return apiError.ReturnWithError(c)
		}
		serviceName := c.QueryParam("service")
		podName := c.QueryParam("podName")

		// Parse query parameters to needed types
		dateFrom := time.UnixMilli(dateFromMSec).UTC()
		dateTo := time.UnixMilli(dateToMSec).UTC()

		filesLocations, err := requestProcessor.TdTopDumpDownloadFiles(ctx, dateFrom, dateTo, namespace, serviceName, podName, dumpType)
		if err != nil {
			return NewAPIError(500, err).ReturnWithError(c)
		}

		fileName := url.PathEscape(fmt.Sprintf("%sUTC-%sUTC.%s.txt.zip", task.FileNameInPV(dateFrom), task.FileNameInPV(dateTo), dumpType))
		c.Response().Header().Add("Content-Type", "application/zip")
		c.Response().Header().Add("Content-Disposition", "attachment; filename="+fileName+";")

		z := zip.NewWriter(c.Response().Writer)
		defer z.Close()

		for _, fileLocation := range filesLocations {
			if fileLocation.PathToZip == "" {
				for _, file := range fileLocation.PathToFiles {
					fileName := filepath.Base(file)
					podName := filepath.Base(filepath.Dir(file))
					fileNameInZip := fmt.Sprintf("%s/%s", podName, fileName)
					zf, err := z.Create(fileNameInZip)
					if err != nil {
						log.Error(ctx, err, "Error creating file with name %s in zip archive", fileNameInZip)
						continue
					}
					f, err := os.Open(file)
					if err != nil {
						log.Error(ctx, err, "Error opening file %s", file)
						continue
					}
					defer f.Close()
					io.Copy(zf, f)
				}
			} else {
				zipReader, err := zip.OpenReader(fileLocation.PathToZip)
				if err != nil {
					log.Error(ctx, err, "Error openning zip %s to collect dumps", fileLocation.PathToZip)
					continue
				}
				defer zipReader.Close()

				for _, file := range fileLocation.PathToFiles {
					fileName := filepath.Base(file)
					podName := filepath.Base(filepath.Dir(file))
					fileNameInZip := filepath.Join(podName, fileName)
					zf, err := z.Create(fileNameInZip)
					if err != nil {
						log.Error(ctx, err, "Error creating file with name %s in zip archive", fileNameInZip)
						continue
					}
					// Separator replacement is needed for windows run, because Open requires '/' separator
					f, err := zipReader.Open(filepath.ToSlash(file))
					if err != nil {
						log.Error(ctx, err, "Error opening file %s in zip %s", file, fileLocation.PathToZip)
						continue
					}

					defer f.Close()
					io.Copy(zf, f)
				}
			}
		}
		return nil
	}
}
