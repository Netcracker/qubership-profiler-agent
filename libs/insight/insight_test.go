package insight

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/clock"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reply is one canned answer of the replay server: a status, an optional
// Retry-After, and a fixture file from testdata.
type reply struct {
	status     int
	retryAfter string
	fixture    string
	body       []byte // used instead of fixture when set
}

// replay serves canned answers in order and records every request. Fixtures
// are synthetic; they follow the shapes of 02-read-contract §2.3, §2.7, §8.
type replay struct {
	t       *testing.T
	mu      sync.Mutex
	replies []reply
	reqs    []*http.Request
}

func (r *replay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reqs = append(r.reqs, req.Clone(context.Background()))
	require.NotEmpty(r.t, r.replies, "unexpected request %s", req.URL)
	next := r.replies[0]
	r.replies = r.replies[1:]
	body := next.body
	if body == nil {
		var err error
		body, err = os.ReadFile(filepath.Join("testdata", next.fixture))
		require.NoError(r.t, err)
	}
	if next.status == http.StatusOK {
		w.Header().Set("Content-Type", "application/json")
	} else {
		w.Header().Set("Content-Type", "application/problem+json")
	}
	if next.retryAfter != "" {
		w.Header().Set("Retry-After", next.retryAfter)
	}
	w.WriteHeader(next.status)
	_, _ = w.Write(body)
}

func newReplay(t *testing.T, opts []Option, replies ...reply) (*Client, *replay) {
	t.Helper()
	r := &replay{t: t, replies: replies}
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL+"/api/v1/", opts...)
	require.NoError(t, err)
	return c, r
}

func ok(fixture string) reply { return reply{status: http.StatusOK, fixture: fixture} }

func TestNewRejectsAURLThatIsNotHTTP(t *testing.T) {
	for _, u := range []string{"", "localhost:8080", "ftp://host", "http://"} {
		_, err := New(u)
		assert.Error(t, err, u)
	}
}

func TestServicesGroupsPodsByNamespaceAndService(t *testing.T) {
	c, r := newReplay(t, nil, ok("pods.json"))
	res, err := c.Services(context.Background(), ServicesInput{})
	require.NoError(t, err)

	require.Len(t, res.Namespaces, 2)
	prod := res.Namespaces[0]
	assert.Equal(t, "prod", prod.Name)
	require.Len(t, prod.Services, 2)
	assert.Equal(t, "billing", prod.Services[0].Name, "services sort by name")
	billing := prod.Services[0].Pods[0]
	assert.Equal(t, "billing-7f8c", billing.Name)
	require.Len(t, billing.Restarts, 2)
	assert.True(t, billing.Restarts[0].StartedAt.Before(billing.Restarts[1].StartedAt), "restarts sort oldest first")
	assert.Equal(t, time.UnixMilli(1760410000000).UTC(), billing.Restarts[0].StartedAt)

	require.Len(t, r.reqs, 1)
	assert.Equal(t, "/api/v1/pods", r.reqs[0].URL.Path)
	assert.NotEmpty(t, r.reqs[0].URL.Query().Get("from"))
}

func TestServicesFiltersByIdentity(t *testing.T) {
	c, _ := newReplay(t, nil, ok("pods.json"))
	res, err := c.Services(context.Background(), ServicesInput{Filters: Filters{Conditions: []string{"service=billing"}}})
	require.NoError(t, err)
	require.Len(t, res.Namespaces, 2, "billing exists in prod and stage")
	for _, ns := range res.Namespaces {
		require.Len(t, ns.Services, 1)
		assert.Equal(t, "billing", ns.Services[0].Name)
	}

	_, err = c.Services(context.Background(), ServicesInput{Filters: Filters{Conditions: []string{"duration>1s"}}})
	assert.ErrorContains(t, err, "namespace, service, and pod conditions only")
}

func TestCallsSendsTheFiltersUnchangedAndMapsRows(t *testing.T) {
	c, r := newReplay(t, nil, ok("calls.json"))
	res, err := c.Calls(context.Background(), CallsInput{
		Window:  Window{Since: 30 * time.Minute},
		Filters: Filters{Conditions: []string{"request_id=abc123", "duration > 1s"}, Service: "prod/billing"},
	})
	require.NoError(t, err)

	q := r.reqs[0].URL.Query()
	assert.Equal(t, "/api/v1/calls", r.reqs[0].URL.Path)
	assert.Equal(t, []string{"request_id=abc123", "duration > 1s", "namespace=prod", "service=billing"}, q["filter"],
		"conditions go as written, the service shorthand expanded")
	assert.Equal(t, "20", q.Get("limit"), "the default page")
	require.NotNil(t, res.Window)
	assert.Equal(t, 30*time.Minute, res.Window.To.Sub(res.Window.From))

	require.Len(t, res.Calls, 2)
	call := res.Calls[0]
	assert.Equal(t, "prod:billing:billing-7f8c:1760420000000:5:12340:0@1760433000123@any_error", call.Ref)
	assert.Equal(t, int32(2470), call.DurationMs)
	assert.Equal(t, int64(1900), call.WaitMs)
	assert.Equal(t, int32(120), call.SuspendMs)
	assert.True(t, call.Error)
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", call.TraceID)
	assert.Equal(t, "abc123", call.RequestID)
	assert.Equal(t, "POST http://billing:8080/charges", call.HTTP)
	assert.Equal(t, "buffer_full", res.Calls[1].Truncated)
	assert.Empty(t, res.Calls[1].HTTP)
	cur, err := decodeCallsCursor(res.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, "cursor-page-2", cur.Server, "the server's cursor rides inside the client's")
	assert.Equal(t, q["filter"], cur.Conditions, "the cursor records the conditions page 1 sent")
	assert.Equal(t, []string{"collector 10.0.0.7 calls: timeout"}, res.Partial)
}

func TestCallsCursorPageReSendsTheConditionsButNotTheWindow(t *testing.T) {
	c, r := newReplay(t, nil, ok("calls.json"))
	cursor := encodeCallsCursor("cursor-page-2", []string{"namespace=prod", "service=billing"})
	res, err := c.Calls(context.Background(), CallsInput{
		Cursor: cursor, Filters: Filters{Service: "prod/billing"}, Window: Window{Since: time.Minute},
	})
	require.NoError(t, err)
	assert.Equal(t, url.Values{
		"cursor": {"cursor-page-2"}, "limit": {"20"}, "filter": {"namespace=prod", "service=billing"},
	}, r.reqs[0].URL.Query(), "the server checks the conditions against its frozen query and keeps its window")
	assert.Nil(t, res.Window, "a cursor page runs over the window of page 1")
	assert.False(t, res.Restarted)
}

func TestCallsCursorPageWithoutFiltersUsesTheCursorFilters(t *testing.T) {
	c, r := newReplay(t, nil, ok("calls.json"), ok("calls.json"))
	cursor := encodeCallsCursor("cursor-page-2", []string{"service=billing", "namespace=prod"})
	res, err := c.Calls(context.Background(), CallsInput{Cursor: cursor})
	require.NoError(t, err)
	assert.Equal(t, []string{"service=billing", "namespace=prod"}, r.reqs[0].URL.Query()["filter"])
	next, err := decodeCallsCursor(res.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, []string{"service=billing", "namespace=prod"}, next.Conditions, "the next cursor keeps them")

	_, err = c.Calls(context.Background(), CallsInput{Cursor: cursor, Filters: Filters{
		Conditions: []string{"namespace = prod", "service in billing"},
	}})
	require.NoError(t, err, "the same conditions in another spelling match")
	assert.Equal(t, []string{"service=billing", "namespace=prod"}, r.reqs[1].URL.Query()["filter"],
		"the cursor's own conditions are the ones authorized and sent")
}

func TestCallsRestartsFromPageOneOnARejectedCursor(t *testing.T) {
	c, r := newReplay(t, nil,
		reply{status: http.StatusBadRequest, fixture: "problem_cursor_rejected.json"},
		ok("calls.json"))
	stale := encodeCallsCursor("stale", []string{"namespace=prod", "service=billing"})
	res, err := c.Calls(context.Background(), CallsInput{Cursor: stale, Filters: Filters{Service: "prod/billing"}})
	require.NoError(t, err)
	assert.True(t, res.Restarted)
	require.NotNil(t, res.Window)
	require.Len(t, r.reqs, 2)
	assert.Empty(t, r.reqs[1].URL.Query().Get("cursor"))
	assert.Equal(t, []string{"namespace=prod", "service=billing"}, r.reqs[1].URL.Query()["filter"])
}

func TestCallsSendsTheOrder(t *testing.T) {
	c, r := newReplay(t, nil, ok("calls.json"), ok("calls.json"), ok("calls.json"))
	_, err := c.Calls(context.Background(), CallsInput{Order: OrderSlowest})
	require.NoError(t, err)
	assert.Equal(t, "duration_desc", r.reqs[0].URL.Query().Get("order"))

	_, err = c.Calls(context.Background(), CallsInput{})
	require.NoError(t, err)
	assert.False(t, r.reqs[1].URL.Query().Has("order"), "the default order sends nothing")

	cursor := encodeCallsCursor("cursor-page-2", nil)
	_, err = c.Calls(context.Background(), CallsInput{Cursor: cursor, Order: OrderSlowest})
	require.NoError(t, err)
	assert.Equal(t, "duration_desc", r.reqs[2].URL.Query().Get("order"),
		"a cursor page re-sends the order, so the server checks it against page 1")

	_, err = c.Calls(context.Background(), CallsInput{Order: "cpu_desc"})
	assert.ErrorContains(t, err, `not "cpu_desc"`)
}

func TestCallsRestartsFromPageOneOnACursorThatDoesNotDecode(t *testing.T) {
	for _, junk := range []string{
		"cursor-page-2", // a server cursor: Calls did not hand it out
		"***",
		encodeCallsCursor("", nil),
		encodeCallsCursor("x", []string{"service"}),
	} {
		c, r := newReplay(t, nil, ok("calls.json"))
		res, err := c.Calls(context.Background(), CallsInput{Cursor: junk, Filters: Filters{Service: "prod/billing"}})
		require.NoError(t, err, junk)
		assert.True(t, res.Restarted, junk)
		require.Len(t, r.reqs, 1, junk)
		assert.Empty(t, r.reqs[0].URL.Query().Get("cursor"), junk)
		assert.Equal(t, []string{"namespace=prod", "service=billing"}, r.reqs[0].URL.Query()["filter"], junk)
	}
}

func TestCallsChecksFilterSyntaxBeforeAnyRequest(t *testing.T) {
	c, r := newReplay(t, nil)
	for _, f := range []Filters{{Conditions: []string{"service"}}, {Service: "billing"}, {Service: "a/b/c"}} {
		_, err := c.Calls(context.Background(), CallsInput{Filters: f})
		assert.Error(t, err, "%+v", f)
	}
	_, err := c.Calls(context.Background(), CallsInput{Limit: MaxCallsLimit + 1})
	assert.ErrorContains(t, err, "limit")
	assert.Empty(t, r.reqs, "nothing reached the server")
}

// TestCallsPassesAConditionTheClientDoesNotKnow pins decision 7 of the plan:
// the client checks syntax only, so the server decides what is implemented.
func TestCallsPassesAConditionTheClientDoesNotKnow(t *testing.T) {
	c, r := newReplay(t, nil, reply{status: http.StatusBadRequest, fixture: "problem_invalid_request.json"})
	_, err := c.Calls(context.Background(), CallsInput{Filters: Filters{Conditions: []string{"service!=billing"}}})
	require.Error(t, err)
	assert.True(t, IsCode(err, CodeInvalidRequest))
	assert.Contains(t, err.Error(), "!= on service is not implemented", "the server's detail reaches the agent")
	assert.Len(t, r.reqs, 1)
}

func TestErrorCodes(t *testing.T) {
	for _, tc := range []struct {
		fixture string
		status  int
		code    string
		message string
	}{
		{"problem_query_too_wide.json", 400, CodeQueryTooWide, "service=<service>, retention_class=any_error, duration>1s, error=true"},
		{"problem_invalid_request.json", 400, CodeInvalidRequest, "is not implemented (invalid_request, HTTP 400)"},
		{"problem_call_not_found.json", 404, CodeCallNotFound, "aged out of retention"},
		{"problem_trace_unavailable.json", 404, CodeTraceUnavailable, "truncated call"},
		{"problem_cursor_rejected.json", 400, CodeCursorRejected, "cursor expired"},
	} {
		c, r := newReplay(t, nil, reply{status: tc.status, fixture: tc.fixture})
		err := c.getJSON(context.Background(), "/calls", nil, &struct{}{})
		require.Error(t, err, tc.code)
		var apiErr *APIError
		require.True(t, errors.As(err, &apiErr), tc.code)
		assert.Equal(t, tc.code, apiErr.Code)
		assert.Equal(t, tc.status, apiErr.Status)
		assert.Contains(t, err.Error(), tc.message, tc.code)
		assert.Len(t, r.reqs, 1, "%s is not retried", tc.code)
	}
}

func TestQueryTooWideNamesTheDominantClass(t *testing.T) {
	c, _ := newReplay(t, nil, reply{status: 400, fixture: "problem_query_too_wide.json"})
	_, err := c.Calls(context.Background(), CallsInput{})
	assert.ErrorContains(t, err, "Estimated scan by class: short_clean 3072 MB, long_clean 100 MB")
}

func TestReadBudgetExhaustedIsRetriedOnce(t *testing.T) {
	busy := reply{status: http.StatusServiceUnavailable, retryAfter: "0", fixture: "problem_read_budget_exhausted.json"}

	c, r := newReplay(t, nil, busy, ok("calls.json"))
	_, err := c.Calls(context.Background(), CallsInput{})
	require.NoError(t, err, "one retry after Retry-After")
	assert.Len(t, r.reqs, 2)

	c, r = newReplay(t, nil, busy, busy)
	_, err = c.Calls(context.Background(), CallsInput{})
	assert.ErrorContains(t, err, "the profiler is busy")
	assert.Len(t, r.reqs, 2, "no second retry")

	c, r = newReplay(t, nil, reply{status: 503, retryAfter: "60", fixture: "problem_read_budget_exhausted.json"})
	_, err = c.Calls(context.Background(), CallsInput{})
	assert.ErrorContains(t, err, "busy")
	assert.Len(t, r.reqs, 1, "a Retry-After past the cap fails at once")
}

func TestNonProblemErrorKeepsTheStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Error(w, "bad gateway from the ingress", http.StatusBadGateway)
	}))
	defer srv.Close()
	c, err := New(srv.URL)
	require.NoError(t, err)
	_, err = c.Services(context.Background(), ServicesInput{})
	assert.ErrorContains(t, err, "bad gateway from the ingress (HTTP 502)")
}

func TestCredentialIsAttachedWhenPresent(t *testing.T) {
	c, r := newReplay(t, nil, ok("pods.json"), ok("pods.json"))
	_, err := c.Services(context.Background(), ServicesInput{})
	require.NoError(t, err)
	assert.Empty(t, r.reqs[0].Header.Get("Authorization"), "v1 sends no credential by default")

	_, err = c.Services(WithCredential(context.Background(), "Bearer t0ken"), ServicesInput{})
	require.NoError(t, err)
	assert.Equal(t, "Bearer t0ken", r.reqs[1].Header.Get("Authorization"), "the caller's own credential is forwarded")
}

type staticCredential string

func (s staticCredential) Credential(context.Context) (string, error) { return string(s), nil }

func TestCredentialSourceCanBeReplaced(t *testing.T) {
	c, r := newReplay(t, []Option{WithCredentialSource(staticCredential("Bearer service"))}, ok("pods.json"))
	_, err := c.Services(context.Background(), ServicesInput{})
	require.NoError(t, err)
	assert.Equal(t, "Bearer service", r.reqs[0].Header.Get("Authorization"))
}

func TestAuthorizerSeesTheNamespacesOfTheConditions(t *testing.T) {
	var seen [][]string
	deny := errors.New("not allowed")
	authorize := func(_ context.Context, namespaces []string) error {
		seen = append(seen, namespaces)
		if len(namespaces) == 0 {
			return deny
		}
		return nil
	}
	c, r := newReplay(t, []Option{WithAuthorizer(authorize)}, ok("calls.json"))
	_, err := c.Calls(context.Background(), CallsInput{Filters: Filters{
		Conditions: []string{"namespace in prod,stage", "service!=x"}, Service: "prod/billing",
	}})
	require.NoError(t, err)
	_, err = c.Services(context.Background(), ServicesInput{})
	assert.ErrorIs(t, err, deny)
	assert.Equal(t, [][]string{{"prod", "stage"}, nil}, seen)
	assert.Len(t, r.reqs, 1, "a denied operation sends nothing")
}

func TestCallRefRoundTrip(t *testing.T) {
	ref := CallRef{
		PK: model.PK{PodNamespace: "prod", PodService: "billing", PodName: "billing-7f8c", RestartTimeMs: 1,
			TraceFileIndex: 5, BufferOffset: 12340, RecordIndex: 2},
		TsMs: 1760433000123, RetentionClass: model.RetentionAnyError,
	}
	got, err := ParseCallRef(" " + ref.String() + " ")
	require.NoError(t, err)
	assert.Equal(t, ref, got)

	for _, bad := range []string{"", "abc", "a@b", "prod:billing:p:1:5:1:2@x@any_error", "prod:billing:p:1:5:1:2@1@hot", "p:1@1@any_error"} {
		_, err := ParseCallRef(bad)
		assert.Error(t, err, bad)
	}
}

func TestParseWindowAndResolve(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	clock.As(now, func() {
		resolve := func(since, from, to string) ResolvedWindow {
			w, err := ParseWindow(since, from, to)
			require.NoError(t, err)
			r, err := w.Resolve()
			require.NoError(t, err)
			return r
		}
		assert.Equal(t, ResolvedWindow{now.Add(-time.Hour), now}, resolve("", "", ""), "the default is the last hour")
		assert.Equal(t, ResolvedWindow{now.Add(-30 * time.Minute), now}, resolve("30m", "", ""))
		assert.Equal(t, ResolvedWindow{now.Add(-48 * time.Hour), now}, resolve("2d", "", ""))
		assert.Equal(t, ResolvedWindow{now.Add(-2 * time.Hour), now.Add(-time.Hour)},
			resolve("1h", "", "2026-10-14T11:00:00Z"), "since counts back from to")
		assert.Equal(t, ResolvedWindow{now.Add(-3 * time.Hour), now},
			resolve("", "2026-10-14T09:00:00Z", ""), "to defaults to now")
		assert.Equal(t, ResolvedWindow{time.UnixMilli(1760430000000).UTC(), time.UnixMilli(1760433600000).UTC()},
			resolve("", "1760430000000", "1760433600000"), "Unix milliseconds")
	})

	for _, in := range [][3]string{{"1h", "1760430000000", ""}, {"soon", "", ""}, {"-1h", "", ""}, {"", "yesterday", ""}} {
		_, err := ParseWindow(in[0], in[1], in[2])
		assert.Error(t, err, "%v", in)
	}
	w, err := ParseWindow("", "2026-10-14T12:00:00Z", "2026-10-14T11:00:00Z")
	require.NoError(t, err)
	_, err = w.Resolve()
	assert.ErrorContains(t, err, "ends")
}

func TestCallTraceIDReadsEveryTraceParam(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params map[string][]string
		want   string
	}{
		{"shared name", map[string][]string{"trace.id": {"t1"}}, "t1"},
		{"Brave only", map[string][]string{"brave.trace_id": {"t2"}}, "t2"},
		{"B3 only", map[string][]string{"X-B3-TraceId": {"t3"}}, "t3"},
		{"shared name first", map[string][]string{"X-B3-TraceId": {"t3"}, "trace.id": {"t1"}}, "t1"},
		{"empty value skipped", map[string][]string{"trace.id": {""}, "brave.trace_id": {"t2"}}, "t2"},
		{"none", map[string][]string{"x-request-id": {"r1"}}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, toCall(model.CallJSON{Params: tc.params}).TraceID)
		})
	}
}
