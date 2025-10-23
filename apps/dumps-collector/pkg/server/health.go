package server

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func health() func(c echo.Context) error {
	return func(c echo.Context) error {
		return c.String(http.StatusNoContent, "")
	}
}
