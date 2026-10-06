package query

import (
	"net/url"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
)

// pkParam decodes the {pk} path segment. 02 §2.2 pins the segment as
// percent-encoded, and echo hands the parameter over still escaped — a
// JS client's encodeURIComponent turns the ':' separators into %3A, which
// ParsePKPath must not see. Surfaced by the it-e2e query-ui run.
func pkParam(c echo.Context) (model.PK, error) {
	raw, err := url.PathUnescape(c.Param("pk"))
	if err != nil {
		return model.PK{}, errors.Wrap(err, "pk is not percent-encoded correctly")
	}
	return model.ParsePKPath(raw)
}
