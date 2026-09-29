package query

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/clock"
	"github.com/Netcracker/qubership-profiler-backend/libs/httpproblem"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// problemBody decodes the §8 envelope by its wire member names.
type problemBody struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
	Code   string `json:"code"`
}

// requestProblem issues one request and asserts the response is the §8
// envelope with the expected status and code.
func requestProblem(t *testing.T, server *httptest.Server, method, path string, wantStatus int, wantCode string) problemBody {
	t.Helper()
	req, err := http.NewRequest(method, server.URL+path, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, wantStatus, resp.StatusCode, "body: %s", raw)
	assert.Equal(t, httpproblem.ContentType, resp.Header.Get("Content-Type"), "body: %s", raw)
	var body problemBody
	require.NoError(t, json.Unmarshal(raw, &body), "body: %s", raw)
	assert.Equal(t, "about:blank", body.Type)
	assert.Equal(t, wantStatus, body.Status)
	assert.Equal(t, wantCode, body.Code)
	return body
}

// The SPA fallback must not swallow an API route miss. With the SPA embedded
// at the root base — the configuration the shipped image runs — echo
// backtracks an unmatched /api/v1 path onto the catch-all, so without the
// guard in handleUI the client gets index.html with a 200 and parses HTML as
// JSON. Both builds are covered: the npm step is optional, and the two must
// not answer a route miss differently.
func TestAPIRouteMissAnswersTheProblemEnvelope(t *testing.T) {
	uiFS := fstest.MapFS{
		"index.html": &fstest.MapFile{
			Data: []byte(`<!doctype html><title>Profiler</title><script type="module" src="/assets/app-ab12.js"></script>`),
		},
		"assets/app-ab12.js": &fstest.MapFile{Data: []byte("console.log('ui')")},
	}

	for _, tc := range []struct {
		name string
		ui   fstest.MapFS
		// The SPA catch-all is registered for GET alone, so with it in place
		// echo matches the path and rejects the method; with no catch-all
		// there is no route to match at all. 02 §8 documents both rows, and
		// the split only exists in one of the two builds.
		unmatchedPathPOSTStatus int
		unmatchedPathPOSTCode   string
	}{
		{"with the SPA embedded", uiFS, http.StatusMethodNotAllowed, httpproblem.CodeMethodNotAllowed},
		{"without the SPA", nil, http.StatusNotFound, httpproblem.CodeNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{}
			if tc.ui != nil {
				opts.UI = tc.ui
			}
			server := httptest.NewServer(New(opts).Handler())
			defer server.Close()

			t.Run("a path no route matches", func(t *testing.T) {
				requestProblem(t, server, http.MethodGet, "/api/v1/nope",
					http.StatusNotFound, httpproblem.CodeNotFound)
			})
			t.Run("a method no route serves", func(t *testing.T) {
				requestProblem(t, server, http.MethodPost, "/api/v1/calls",
					http.StatusMethodNotAllowed, httpproblem.CodeMethodNotAllowed)
			})
			t.Run("a non-GET request to a path no route matches", func(t *testing.T) {
				requestProblem(t, server, http.MethodPost, "/api/v1/nope",
					tc.unmatchedPathPOSTStatus, tc.unmatchedPathPOSTCode)
			})
			if tc.ui == nil {
				return
			}
			t.Run("a client-side route still reaches the SPA", func(t *testing.T) {
				resp, err := http.Get(server.URL + "/calls")
				require.NoError(t, err)
				raw, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				assert.Contains(t, resp.Header.Get("Content-Type"), "text/html")
				assert.Contains(t, string(raw), "Profiler")
			})
		})
	}
}

// A cursor failure and a caller bug are both 400s, and a paging client reacts
// to them in opposite ways: cursor_rejected restarts from page 1 silently,
// invalid_request surfaces to the user. Nothing but the code separates them,
// so a reworded detail must not be able to swap one for the other.
func TestCursorRejectionCarriesItsOwnCode(t *testing.T) {
	const fromMs, toMs = int64(1_700_000_000_000), int64(1_700_000_060_000)
	window := func() url.Values {
		return url.Values{"from": {"1700000000000"}, "to": {"1700000060000"}}
	}
	frozen := model.CallsQuery{FromMs: fromMs, ToMs: toMs, Method: "com.example.Service.handle"}
	pos := model.Position{TsMs: fromMs + 10, PK: model.PK{
		PodNamespace: "ns", PodService: "svc", PodName: "pod", RestartTimeMs: fromMs,
	}}
	// Minted an hour back so it is past the default 15-minute CursorTTL by a
	// wide margin. Shrinking the TTL instead would race: a cursor issued and
	// replayed inside the same millisecond is not yet expired, and the request
	// would run on to the cold tier.
	var expiredCursor string
	clock.As(time.Now().Add(-time.Hour), func() { expiredCursor = encodeCursor(frozen, pos) })

	for _, tc := range []struct {
		name   string
		params func() url.Values
		code   string
	}{
		{
			name: "a cursor that does not decode",
			params: func() url.Values {
				p := window()
				p.Set("cursor", "not-a-cursor")
				return p
			},
			code: httpproblem.CodeCursorRejected,
		},
		{
			name: "a cursor past its TTL",
			params: func() url.Values {
				p := window()
				p.Set("cursor", expiredCursor)
				return p
			},
			code: httpproblem.CodeCursorRejected,
		},
		{
			name: "a re-sent filter that contradicts the frozen query",
			params: func() url.Values {
				p := window()
				p.Set("cursor", encodeCursor(frozen, pos))
				p.Set("method", "com.example.Other.handle")
				return p
			},
			code: httpproblem.CodeCursorRejected,
		},
		{
			name: "a malformed parameter on page 1",
			params: func() url.Values {
				p := window()
				p.Set("limit", "abc")
				return p
			},
			code: httpproblem.CodeInvalidRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(New(Options{}).Handler())
			defer server.Close()

			body := requestProblem(t, server, http.MethodGet,
				"/api/v1/calls?"+tc.params().Encode(), http.StatusBadRequest, tc.code)
			assert.NotEmpty(t, body.Detail)
		})
	}
}
