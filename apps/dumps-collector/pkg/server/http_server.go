package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/task"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func StartHttpServer(ctx context.Context, requestProcessor *task.RequestProcessor, bindAddress string) error {
	e := echo.New()
	e.Server.BaseContext = func(_ net.Listener) context.Context {
		return ctx
	}
	e.Use(middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogStatus:   true,
		LogURI:      true,
		LogError:    true,
		HandleError: true,
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
			ctx := c.Request().Context()
			if v.Error != nil {
				log.Error(ctx, v.Error, "Request error, uri = %s, status = %d", v.URI, v.Status)
			} else {
				log.Debug(ctx, "Received request, uri = %s, status = %d", v.URI, v.Status)
			}
			return nil
		},
	}))
	e.Use(echoprometheus.NewMiddleware("cloud_profiler_dumps_collector"))

	e.GET("/esc/health", health())
	e.GET("/esc/metrics", echoprometheus.NewHandler())
	e.GET("/cdt/v2/download", downloadTdTopDump(requestProcessor))
	e.GET("/cdt/v2/heaps/download/:handle", downloadHeapDump(requestProcessor))

	return e.Start(bindAddress)
}

func getQueryString(c echo.Context, name string) (string, *APIError) {
	str := c.QueryParam(name)
	if str == "" {
		return "", NewAPIError(
			http.StatusInternalServerError,
			fmt.Errorf("required query parameter '%s' is not present", name),
		)
	}
	return str, nil
}

// getPathParam extracts a required path parameter from the Echo context.
// If the parameter is missing or empty, it returns an APIError with status 500.
func getPathParam(c echo.Context, name string) (string, *APIError) {
	value := c.Param(name)
	if value == "" {
		return "", NewAPIError(
			http.StatusInternalServerError,
			fmt.Errorf("missing required path param: %s", name),
		)
	}
	return value, nil
}

func getQueryInt64(c echo.Context, name string) (int64, *APIError) {
	str, apiError := getQueryString(c, name)
	if apiError != nil {
		return 0, apiError
	}
	val, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return 0, NewAPIError(
			http.StatusInternalServerError,
			fmt.Errorf("error converting parameter %s to int: %+w", name, err),
		)
	}
	return val, nil
}

func getQueryDumpType(c echo.Context, name string) (model.DumpType, *APIError) {
	str, apiError := getQueryString(c, name)
	if apiError != nil {
		return model.HeapDumpType, apiError
	}
	switch str {
	case string(model.TdDumpType):
		return model.TdDumpType, nil
	case string(model.TopDumpType):
		return model.TopDumpType, nil
	}
	return model.HeapDumpType, NewAPIError(
		http.StatusInternalServerError,
		fmt.Errorf("unsupported dump type %s", str),
	)
}
