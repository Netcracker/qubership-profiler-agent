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

// The SPA surface (07 §6): embedded files serve as-is, client-side routes fall
// back to index.html, and hashed assets carry the immutable cache tag. The
// serving prefix follows the build-time base, read from index.html — root by
// default, or a sub-path like /ui — so both are exercised.
func TestUIServing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		base  string // URL prefix: "" for root, "/ui" for a sub-path
		index string
	}{
		{"root", "", `<!doctype html><title>Profiler</title><script type="module" src="/assets/app-ab12.js"></script>`},
		{"sub-path", "/ui", `<!doctype html><title>Profiler</title><script type="module" src="/ui/assets/app-ab12.js"></script>`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uiFS := fstest.MapFS{
				"index.html":         &fstest.MapFile{Data: []byte(tc.index)},
				"assets/app-ab12.js": &fstest.MapFile{Data: []byte("console.log('ui')")},
			}
			svc := New(Options{UI: uiFS})
			require.Equal(t, tc.base, svc.uiPrefix, "prefix must be read from index.html")
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

			t.Run("index at the base", func(t *testing.T) {
				indexPaths := []string{tc.base + "/"}
				if tc.base != "" {
					indexPaths = append(indexPaths, tc.base)
				}
				for _, p := range indexPaths {
					resp, body := get(p)
					assert.Equal(t, http.StatusOK, resp.StatusCode, p)
					assert.Contains(t, body, "Profiler", p)
					assert.Contains(t, resp.Header.Get("Content-Type"), "text/html", p)
					assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"), p)
				}
			})

			t.Run("client-side routes fall back to index.html", func(t *testing.T) {
				for _, p := range []string{tc.base + "/calls", tc.base + "/pods", tc.base + "/tree/ns:svc:pod:1:2:3:4", tc.base + "/no/such/route"} {
					resp, body := get(p)
					assert.Equal(t, http.StatusOK, resp.StatusCode, p)
					assert.Contains(t, body, "Profiler", p)
				}
			})

			t.Run("hashed assets are immutable", func(t *testing.T) {
				resp, body := get(tc.base + "/assets/app-ab12.js")
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				assert.Contains(t, body, "console.log")
				assert.Contains(t, resp.Header.Get("Content-Type"), "javascript")
				assert.Contains(t, resp.Header.Get("Cache-Control"), "immutable")
			})

			t.Run("traversal resolves to the SPA fallback, not the filesystem", func(t *testing.T) {
				req, err := http.NewRequest(http.MethodGet, server.URL+tc.base+"/x/y", nil)
				require.NoError(t, err)
				// A raw dotted path never reaches the handler canonically, so force
				// one through the URL opaque path.
				req.URL.Path = tc.base + "/../go.mod"
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
				// Validation 400, not a UI fallback: the specific route wins over
				// the catch-all.
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			})
		})
	}
}

// Without UI assets the routes stay unregistered and both the root and /ui are
// plain 404s.
func TestUIDisabled(t *testing.T) {
	svc := New(Options{})
	server := httptest.NewServer(svc.Handler())
	defer server.Close()

	for _, p := range []string{"/", "/ui", "/calls"} {
		resp, err := http.Get(server.URL + p)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, p)
	}
}
