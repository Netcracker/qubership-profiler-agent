package server

import (
	"archive/zip"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/task"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/labstack/echo/v5"
)

// dispositionQuoter escapes a file name for the quoted filename parameter of
// Content-Disposition, as echo.Context.Attachment does.
var dispositionQuoter = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

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
			// Not c.Attachment: Echo v5 opens that path through Echo.Filesystem,
			// which is rooted at the working directory, and the dump lives on
			// the PV outside it.
			file := fileLocation.PathToFiles[0]
			name := filepath.Base(file)
			c.Response().Header().Set(echo.HeaderContentDisposition,
				fmt.Sprintf(`attachment; filename="%s"`, dispositionQuoter.Replace(name)))
			return c.FileFS(name, os.DirFS(filepath.Dir(file)))
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
