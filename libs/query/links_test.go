package query

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const linksFixture = `[
	{"label": "Open trace · Tempo", "kind": "trace",
	 "url_template": "https://grafana.example/explore?trace={trace_id}"},
	{"label": "Logs · Grafana", "kind": "logs",
	 "url_template": "https://grafana.example/logs?ns={namespace}&svc={service}&pod={pod}&from={from_ms}&to={to_ms}"}
]`

func TestParseLinks(t *testing.T) {
	t.Run("accepts every placeholder of the contract", func(t *testing.T) {
		links, err := ParseLinks(linksFixture)
		require.NoError(t, err)
		assert.Equal(t, []Link{
			{Label: "Open trace · Tempo", Kind: LinkKindTrace,
				URLTemplate: "https://grafana.example/explore?trace={trace_id}"},
			{Label: "Logs · Grafana", Kind: LinkKindLogs,
				URLTemplate: "https://grafana.example/logs?ns={namespace}&svc={service}&pod={pod}&from={from_ms}&to={to_ms}"},
		}, links)
	})

	t.Run("accepts a placeholder in the host and in the path", func(t *testing.T) {
		_, err := ParseLinks(`[{"label": "Logs", "kind": "logs",
			"url_template": "https://logs-{namespace}.example/pods/{pod}"}]`)
		require.NoError(t, err)
	})

	t.Run("treats an empty value and an empty array as no links", func(t *testing.T) {
		for _, raw := range []string{"", "  ", "[]"} {
			links, err := ParseLinks(raw)
			require.NoError(t, err, "value %q", raw)
			assert.Empty(t, links, "value %q", raw)
		}
	})
}

// A malformed entry must stop startup with a message that names it: the
// operator sees the position and the label, not just "invalid links".
func TestParseLinksRejects(t *testing.T) {
	good := `{"label": "Open trace", "kind": "trace", "url_template": "https://tempo.example/{trace_id}"}`
	cases := []struct {
		name  string
		entry string
		want  []string
	}{
		{
			name:  "an unknown placeholder",
			entry: `{"label": "Pod logs", "kind": "logs", "url_template": "https://logs.example/?pod={pod_name}"}`,
			want:  []string{`entry 2 (label "Pod logs")`, "unknown placeholder {pod_name}", "{trace_id}"},
		},
		{
			name:  "an empty placeholder",
			entry: `{"label": "Pod logs", "kind": "logs", "url_template": "https://logs.example/?pod={}"}`,
			want:  []string{`entry 2 (label "Pod logs")`, "unknown placeholder {}"},
		},
		{
			name:  "an unmatched brace",
			entry: `{"label": "Pod logs", "kind": "logs", "url_template": "https://logs.example/?pod={pod"}`,
			want:  []string{`entry 2 (label "Pod logs")`, "unmatched brace"},
		},
		{
			name:  "an unknown kind",
			entry: `{"label": "Metrics", "kind": "metrics", "url_template": "https://grafana.example/d/{pod}"}`,
			want:  []string{`entry 2 (label "Metrics")`, `unknown kind "metrics"`, `"trace"`, `"logs"`},
		},
		{
			name:  "a missing kind",
			entry: `{"label": "Metrics", "url_template": "https://grafana.example/d/{pod}"}`,
			want:  []string{`entry 2 (label "Metrics")`, `unknown kind ""`},
		},
		{
			name:  "an empty label",
			entry: `{"label": " ", "kind": "logs", "url_template": "https://logs.example/?pod={pod}"}`,
			want:  []string{`entry 2 (label " ")`, "label is empty"},
		},
		{
			name:  "an empty template",
			entry: `{"label": "Pod logs", "kind": "logs", "url_template": ""}`,
			want:  []string{`entry 2 (label "Pod logs")`, "url_template is empty"},
		},
		{
			name:  "a URL that does not parse",
			entry: `{"label": "Pod logs", "kind": "logs", "url_template": "https://logs.example:port/?pod={pod}"}`,
			want:  []string{`entry 2 (label "Pod logs")`, `"https://logs.example:port/?pod={pod}" is not a URL`},
		},
		{
			name:  "a relative URL",
			entry: `{"label": "Pod logs", "kind": "logs", "url_template": "/logs?pod={pod}"}`,
			want:  []string{`entry 2 (label "Pod logs")`, "not an absolute http(s) URL"},
		},
		{
			name:  "a scheme the UI must not turn into a link",
			entry: `{"label": "Pod logs", "kind": "logs", "url_template": "javascript:alert('{pod}')"}`,
			want:  []string{`entry 2 (label "Pod logs")`, "not an absolute http(s) URL"},
		},
		{
			name:  "a placeholder as the whole URL",
			entry: `{"label": "Pod logs", "kind": "logs", "url_template": "{pod}"}`,
			want:  []string{`entry 2 (label "Pod logs")`, "not an absolute http(s) URL"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseLinks("[" + good + "," + tc.entry + "]")
			require.Error(t, err)
			for _, want := range tc.want {
				assert.Contains(t, err.Error(), want)
			}
		})
	}

	t.Run("a misspelled key", func(t *testing.T) {
		_, err := ParseLinks(`[{"label": "Open trace", "kind": "trace", "urlTemplate": "https://tempo.example/{trace_id}"}]`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "urlTemplate")
	})

	t.Run("a value that is not a JSON array", func(t *testing.T) {
		for _, raw := range []string{good, "not json", "[" + good + "] trailing"} {
			_, err := ParseLinks(raw)
			require.Error(t, err, "value %q", raw)
			assert.Contains(t, err.Error(), "links:", "value %q", raw)
		}
	})
}

func TestConfigEndpointLinks(t *testing.T) {
	get := func(t *testing.T, cfg Config) map[string]json.RawMessage {
		t.Helper()
		server := httptest.NewServer(New(Options{Config: cfg}).Handler())
		defer server.Close()

		resp, err := http.Get(server.URL + "/api/v1/config")
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var body map[string]json.RawMessage
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		return body
	}

	t.Run("reports the configured links in order", func(t *testing.T) {
		links, err := ParseLinks(linksFixture)
		require.NoError(t, err)

		var got []map[string]string
		require.NoError(t, json.Unmarshal(get(t, Config{Links: links})["links"], &got))
		assert.Equal(t, []map[string]string{
			{"label": "Open trace · Tempo", "kind": "trace",
				"url_template": "https://grafana.example/explore?trace={trace_id}"},
			{"label": "Logs · Grafana", "kind": "logs",
				"url_template": "https://grafana.example/logs?ns={namespace}&svc={service}&pod={pod}&from={from_ms}&to={to_ms}"},
		}, got)
	})

	t.Run("reports an empty array when no link is configured", func(t *testing.T) {
		assert.JSONEq(t, `[]`, string(get(t, Config{})["links"]))
	})
}
