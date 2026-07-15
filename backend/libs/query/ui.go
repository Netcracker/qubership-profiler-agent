package query

import (
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"

	"github.com/labstack/echo/v4"
)

// handleUI serves the embedded single-page app (07 §6): real files come from
// the embedded dist, anything else falls back to index.html so client-side
// routes (/ui/calls, /ui/tree/:pk) deep-link and refresh.
func (s *Service) handleUI(c echo.Context) error {
	p := strings.TrimPrefix(c.Request().URL.Path, "/ui")
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		p = "index.html"
	}
	body, err := fs.ReadFile(s.ui, p)
	if err != nil {
		// Includes traversal attempts: io/fs rejects any non-canonical path.
		p = "index.html"
		if body, err = fs.ReadFile(s.ui, p); err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, "ui assets lack index.html")
		}
	}
	contentType := mime.TypeByExtension(path.Ext(p))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	// Vite content-hashes everything under assets/, so those cache forever;
	// index.html revalidates to pick up a redeploy.
	if strings.HasPrefix(p, "assets/") {
		c.Response().Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		c.Response().Header().Set("Cache-Control", "no-cache")
	}
	return c.Blob(http.StatusOK, contentType, body)
}
