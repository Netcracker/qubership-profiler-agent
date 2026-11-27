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

	t.Run("accepts a brace that is not a placeholder as part of the URL", func(t *testing.T) {
		for _, tc := range []struct{ name, template string }{
			{"a LogsQL stream selector beside a placeholder",
				`https://vl.example/select/vmui/?query={app="x"} AND trace_id:{trace_id}`},
			{"a LogQL stream selector around a placeholder",
				`https://grafana.example/explore?query={kubernetes_pod_name="{pod}"}`},
			{"a Grafana Explore JSON state",
				`https://grafana.example/explore?left={"queries":[{"expr":"{pod=\"{pod}\"}"}],"range":{"from":"{from_ms}","to":"{to_ms}"}}`},
			{"an empty pair of braces", `https://logs.example/?q={}&pod={pod}`},
			{"a regexp quantifier", `https://logs.example/?q=a{2,3}&pod={pod}`},
			{"a regexp Unicode class", `https://logs.example/?q=\p{Greek}&pod={pod}`},
			{"a regexp hex escape", `https://logs.example/?q=\x{a0}&pod={pod}`},
			{"a bare name that is not a placeholder", `https://logs.example/?pod={pod_name}`},
			{"a bare name with its opening brace encoded", `https://logs.example/?q=%7Bmessage}&pod={pod}`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := ParseLinks(`[{"label": "Logs", "kind": "logs", "url_template": ` + jsonString(t, tc.template) + `}]`)
				require.NoError(t, err)
			})
		}
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

// A bare name in braces that is not a placeholder is valid URL text, so it
// passes ParseLinks; the warning is the only report of a misspelled one.
func TestLinkWarnings(t *testing.T) {
	warn := func(t *testing.T, template string) []string {
		t.Helper()
		links, err := ParseLinks(`[{"label": "Open trace", "kind": "trace", "url_template": "https://tempo.example/{trace_id}"},
			{"label": "Pod logs", "kind": "logs", "url_template": ` + jsonString(t, template) + `}]`)
		require.NoError(t, err)
		return LinkWarnings(links)
	}

	for _, tc := range []struct{ name, template, suspect, placeholder string }{
		{"a name that contains a placeholder", `https://logs.example/?pod={pod_name}`, "{pod_name}", "{pod}"},
		{"a name in camel case", `https://logs.example/?trace={traceId}`, "{traceId}", "{trace_id}"},
		{"a name in upper case", `https://logs.example/?pod={POD}`, "{POD}", "{pod}"},
		{"a name with a hyphen", `https://logs.example/?from={from-ms}`, "{from-ms}", "{from_ms}"},
		{"a param name", `https://logs.example/?trace={trace.id}`, "{trace.id}", "{trace_id}"},
		{"a name that starts a placeholder", `https://logs.example/?from={__from}`, "{__from}", "{from_ms}"},
		{"a name repeated in one template", `https://logs.example/?a={pod_name}&b={pod_name}`, "{pod_name}", "{pod}"},
	} {
		t.Run("names "+tc.name, func(t *testing.T) {
			warnings := warn(t, tc.template)
			require.Len(t, warnings, 1, "LinkWarnings for %q", tc.template)
			for _, want := range []string{"PROFILER_LINKS", `entry 2 (label "Pod logs")`, tc.suspect, tc.placeholder, "%7B"} {
				assert.Contains(t, warnings[0], want)
			}
		})
	}

	for _, tc := range []struct{ name, template string }{
		{"every placeholder", `https://logs.example/?ns={namespace}&svc={service}&pod={pod}&from={from_ms}&to={to_ms}`},
		{"a stream selector", `https://logs.example/?q={app="x"}&pod={pod}`},
		{"an encoded opening brace", `https://logs.example/?q=%7Bmessage}`},
		{"a regexp quantifier", `https://logs.example/?q=a{2,3}`},
		{"a regexp hex escape", `https://logs.example/?q=\x{a0}`},
		{"a regexp Unicode class", `https://logs.example/?q=\p{Greek}`},
		{"a name unrelated to every placeholder", `https://logs.example/?q={job}`},
	} {
		t.Run("stays quiet for "+tc.name, func(t *testing.T) {
			assert.Empty(t, warn(t, tc.template), "LinkWarnings for %q", tc.template)
		})
	}
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

// jsonString encodes s as a JSON string literal, so a template with quotes
// and backslashes can be spliced into a PROFILER_LINKS value.
func jsonString(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	require.NoError(t, err)
	return string(b)
}
