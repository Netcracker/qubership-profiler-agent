package query

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GET /api/v1/config surfaces deployment-specific values the UI cannot
// derive on its own, currently just the dumps-collector link-out base
// (PR 708 review #18).
func TestConfigEndpoint(t *testing.T) {
	t.Run("reports the configured dumps-collector URL", func(t *testing.T) {
		svc := New(Options{Config: Config{DumpsCollectorURL: "https://dumps-collector-myns.example.com"}})
		server := httptest.NewServer(svc.Handler())
		defer server.Close()

		resp, err := http.Get(server.URL + "/api/v1/config")
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var body configResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		assert.Equal(t, "https://dumps-collector-myns.example.com", body.DumpsCollectorURL)
	})

	t.Run("drops an unsafe non-http(s) URL so the UI cannot render it (PR 708 review #10)", func(t *testing.T) {
		for _, unsafe := range []string{"javascript:alert(1)", "not a url", "/relative/path", "ftp://host/x"} {
			svc := New(Options{Config: Config{DumpsCollectorURL: unsafe}})
			server := httptest.NewServer(svc.Handler())

			resp, err := http.Get(server.URL + "/api/v1/config")
			require.NoError(t, err)
			var body configResponse
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
			_ = resp.Body.Close()
			server.Close()
			assert.Equal(t, "", body.DumpsCollectorURL, "unsafe value %q must be dropped", unsafe)
		}
	})

	t.Run("reports an empty URL when unconfigured, not an error", func(t *testing.T) {
		svc := New(Options{})
		server := httptest.NewServer(svc.Handler())
		defer server.Close()

		resp, err := http.Get(server.URL + "/api/v1/config")
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var body configResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		assert.Equal(t, "", body.DumpsCollectorURL)
	})
}
