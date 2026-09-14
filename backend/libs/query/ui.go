package query

import (
	"io/fs"
	"mime"
	"net/http"
	"path"
	"regexp"
	"strings"

	"github.com/labstack/echo/v4"
)

// The first asset URL in the built index.html carries the Vite base, e.g.
// `/assets/...` for a root build or `/ui/assets/...` under a sub-path. That is
// the one place the UI base is configured (vite base); the binary reads it back
// out rather than carrying its own copy.
var uiAssetBaseRE = regexp.MustCompile(`(?:src|href)="(/[^"]*?)assets/`)

// apiPrefix is the external API's URL prefix with no surrounding slashes, in
// the shape handleUI compares a trimmed request path against. It must stay in
// step with the routes Service.routes registers.
const apiPrefix = "api/v1"

// uiPrefix is the URL prefix the SPA is served under, read from the built
// index.html: "" for a root build ("/assets/...") or e.g. "/ui" for
// "/ui/assets/...". It defaults to root when the base cannot be read.
func uiPrefix(fsys fs.FS) string {
	body, err := fs.ReadFile(fsys, "index.html")
	if err != nil {
		return ""
	}
	m := uiAssetBaseRE.FindSubmatch(body)
	if m == nil {
		return ""
	}
	return strings.TrimSuffix(string(m[1]), "/")
}

// handleUI serves the embedded single-page app (07 §6): real files come from
// the embedded dist, anything else falls back to index.html so client-side
// routes ({base}calls, {base}tree/:pk) deep-link and refresh.
func (s *Service) handleUI(c echo.Context) error {
	p := strings.TrimPrefix(c.Request().URL.Path, s.uiPrefix)
	p = strings.TrimPrefix(p, "/")
	// A root-base build registers the catch-all at "/", so echo backtracks an
	// unmatched /api/v1 path onto it. That is a route miss, not a deep link:
	// answer the §8 envelope rather than the SPA shell, or an API client
	// parses index.html as JSON. The check reads the trimmed path, so under a
	// sub-path base it can only see <base>/api/v1/..., which the SPA owns no
	// route for either — its routes are calls, pods, and tree/:pk.
	if p == apiPrefix || strings.HasPrefix(p, apiPrefix+"/") {
		return echo.ErrNotFound
	}
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
