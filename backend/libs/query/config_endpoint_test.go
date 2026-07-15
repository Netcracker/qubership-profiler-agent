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
