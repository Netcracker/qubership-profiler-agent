package query

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A percent-encoded pk segment (a JS encodeURIComponent escapes the ':'
// separators to %3A) must decode before ParsePKPath (02 §2.2).
func TestPKParamDecodesPercentEncoding(t *testing.T) {
	e := echo.New()
	for _, segment := range []string{
		"ns:svc:pod-1:1714060800000:5:12340:0",
		"ns%3Asvc%3Apod-1%3A1714060800000%3A5%3A12340%3A0",
	} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		c := e.NewContext(req, httptest.NewRecorder())
		c.SetParamNames("pk")
		c.SetParamValues(segment)

		pk, err := pkParam(c)
		require.NoError(t, err, segment)
		assert.Equal(t, "ns", pk.PodNamespace, segment)
		assert.Equal(t, "pod-1", pk.PodName, segment)
		assert.Equal(t, int64(1714060800000), pk.RestartTimeMs, segment)
		assert.Equal(t, int32(12340), pk.BufferOffset, segment)
	}
}
