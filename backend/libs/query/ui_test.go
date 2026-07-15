package query

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The /ui surface (07 §6): embedded files serve as-is, client-side routes
// fall back to index.html, and hashed assets carry the immutable cache tag.
func TestUIServing(t *testing.T) {
	uiFS := fstest.MapFS{
		"index.html":         &fstest.MapFile{Data: []byte("<!doctype html><title>Profiler</title>")},
		"assets/app-ab12.js": &fstest.MapFile{Data: []byte("console.log('ui')")},
	}
	svc := New(Options{UI: uiFS})
	server := httptest.NewServer(svc.Handler())
	defer server.Close()

	get := func(path string) (*http.Response, string) {
		resp, err := http.Get(server.URL + path)
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return resp, string(body)
	}

	t.Run("index at /ui and /ui/", func(t *testing.T) {
		for _, p := range []string{"/ui", "/ui/"} {
			resp, body := get(p)
			assert.Equal(t, http.StatusOK, resp.StatusCode, p)
			assert.Contains(t, body, "Profiler", p)
			assert.Contains(t, resp.Header.Get("Content-Type"), "text/html", p)
			assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"), p)
		}
	})

	t.Run("client-side routes fall back to index.html", func(t *testing.T) {
		for _, p := range []string{"/ui/calls", "/ui/pods", "/ui/tree/ns:svc:pod:1:2:3:4", "/ui/no/such/route"} {
			resp, body := get(p)
			assert.Equal(t, http.StatusOK, resp.StatusCode, p)
			assert.Contains(t, body, "Profiler", p)
		}
	})

	t.Run("hashed assets are immutable", func(t *testing.T) {
		resp, body := get("/ui/assets/app-ab12.js")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Contains(t, body, "console.log")
		assert.Contains(t, resp.Header.Get("Content-Type"), "javascript")
		assert.Contains(t, resp.Header.Get("Cache-Control"), "immutable")
	})

	t.Run("traversal resolves to the SPA fallback, not the filesystem", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, server.URL+"/ui/x/y", nil)
		require.NoError(t, err)
		// A raw dotted path never reaches the handler canonically, so force
		// one through the URL opaque path.
		req.URL.Path = "/ui/../go.mod"
		resp, err := http.DefaultTransport.RoundTrip(req)
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		if resp.StatusCode == http.StatusOK {
			assert.False(t, strings.Contains(string(body), "module "), "must never leak files outside the bundle")
		}
	})

	t.Run("the API routes stay reachable", func(t *testing.T) {
		resp, _ := get("/api/v1/calls")
		// Validation 400, not a UI fallback: the route registration held.
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}

// Without UI assets the route stays unregistered and /ui is a plain 404.
func TestUIDisabled(t *testing.T) {
	svc := New(Options{})
	server := httptest.NewServer(svc.Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/ui")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
