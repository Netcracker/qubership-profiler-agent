// Package ui embeds the built single-page app that the query service serves
// at /ui (07-ui-design.md §6). Build the assets first — `npm run build` in
// this directory; the profiler-backend Dockerfile runs that in a node stage.
package ui

import (
	"embed"
	"io/fs"

	"github.com/pkg/errors"
)

// The committed dist/.gitkeep keeps the pattern valid, so a Go build without
// the npm step still compiles; Dist then reports the assets as missing.
//
//go:embed all:dist
var dist embed.FS

// Dist returns the built asset tree rooted at dist/. It errors when the
// bundle was never built, and the caller decides whether a UI-less binary is
// acceptable (the query command serves the API and merely warns).
func Dist() (fs.FS, error) {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		return nil, errors.Wrap(err, "ui assets")
	}
	if _, err := fs.Stat(sub, "index.html"); err != nil {
		return nil, errors.New("ui assets are not built: run `npm run build` in backend/apps/ui")
	}
	return sub, nil
}
