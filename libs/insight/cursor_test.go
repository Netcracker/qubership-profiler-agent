package insight

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/clock"
	"github.com/Netcracker/qubership-profiler-backend/libs/query"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errNotProd = errors.New("only prod is allowed")

// prodOnly is an Authorizer that allows the prod namespace alone.
func prodOnly(_ context.Context, namespaces []string) error {
	if len(namespaces) == 0 {
		return errNotProd
	}
	for _, ns := range namespaces {
		if ns != "prod" {
			return errNotProd
		}
	}
	return nil
}

// secretCursor lists secret/billing for one page and returns the cursor of
// that listing.
func secretCursor(t *testing.T) string {
	t.Helper()
	c, r := newReplay(t, nil, ok("calls.json"))
	res, err := c.Calls(context.Background(), CallsInput{Filters: Filters{Service: "secret/billing"}})
	require.NoError(t, err)
	require.Len(t, r.reqs, 1)
	require.NotEmpty(t, res.NextCursor)
	return res.NextCursor
}

// TestCallsRejectsACursorFromOtherFilters is the regression for the PR 1095
// review: a cursor of secret/billing passed with prod/billing was
// authorized as prod, sent alone, and read secret through the server's
// frozen query.
func TestCallsRejectsACursorFromOtherFilters(t *testing.T) {
	cursor := secretCursor(t)
	for name, authorize := range map[string]Authorizer{"prod only": prodOnly, "allow all": AllowAll} {
		c, r := newReplay(t, []Option{WithAuthorizer(authorize)})
		_, err := c.Calls(context.Background(), CallsInput{Cursor: cursor, Filters: Filters{Service: "prod/billing"}})
		var mismatch *CursorMismatchError
		require.True(t, errors.As(err, &mismatch), "%s: %v", name, err)
		assert.Equal(t, []string{"namespace=secret", "service=billing"}, mismatch.CursorFilters, name)
		assert.Equal(t, []string{"namespace=prod", "service=billing"}, mismatch.Filters, name)
		assert.EqualError(t, err, "the cursor continues a listing filtered by namespace=secret, service=billing, "+
			"not by namespace=prod, service=billing: pass the cursor with that listing's filters or with none, "+
			"or drop it to start a new listing", name)
		assert.Empty(t, r.reqs, "%s: nothing reached the server", name)
	}

	c, r := newReplay(t, nil)
	_, err := c.Calls(context.Background(), CallsInput{Cursor: cursor, Filters: Filters{Conditions: []string{"duration>1s"}}})
	assert.ErrorAs(t, err, new(*CursorMismatchError), "a narrower set of conditions is another listing too")
	assert.Empty(t, r.reqs)
}

func TestCallsAuthorizesTheNamespacesOfTheCursor(t *testing.T) {
	cursor := secretCursor(t)
	var seen [][]string
	authorize := func(ctx context.Context, namespaces []string) error {
		seen = append(seen, namespaces)
		return prodOnly(ctx, namespaces)
	}
	c, r := newReplay(t, []Option{WithAuthorizer(authorize)})
	_, err := c.Calls(context.Background(), CallsInput{Cursor: cursor})
	require.ErrorIs(t, err, errNotProd, "a cursor passed alone reads under its own filters")
	assert.Equal(t, [][]string{{"secret"}}, seen)
	assert.Empty(t, r.reqs)
}

// emptyStore is a cold tier with no objects.
type emptyStore struct{}

func (emptyStore) List(context.Context, string) ([]cold.ObjectInfo, error) { return nil, nil }

func (emptyStore) Open(context.Context, string) (cold.Object, error) { return nil, cold.ErrNotFound }

func (emptyStore) Get(context.Context, string) ([]byte, error) { return nil, cold.ErrNotFound }

// serverCursor mints a cursor of the query service, in the version 2
// format of libs/query/cursor.go, frozen to the given parameters.
func serverCursor(t *testing.T, params url.Values) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"v":   2,
		"q":   params,
		"pos": model.Position{TsMs: clock.Now().UnixMilli()},
		"iat": clock.Now().UnixMilli(),
	})
	require.NoError(t, err)
	return base64.RawURLEncoding.EncodeToString(body)
}

// TestQueryServiceChecksTheCursorConditions runs the client against the
// real query service. A hand-made client cursor that claims prod while the
// server cursor inside it is frozen to secret is rejected, because Calls
// re-sends the conditions it authorized; the page is page 1 of prod.
func TestQueryServiceChecksTheCursorConditions(t *testing.T) {
	var mu sync.Mutex
	var reqs []url.Values
	api := query.New(query.Options{ColdStore: emptyStore{}}).Handler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		reqs = append(reqs, req.URL.Query())
		mu.Unlock()
		api.ServeHTTP(w, req)
	}))
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, WithAuthorizer(prodOnly))
	require.NoError(t, err)

	now := time.Now()
	frozenTo := func(namespace string) url.Values {
		return url.Values{
			"from":   {strconv.FormatInt(now.Add(-time.Hour).UnixMilli(), 10)},
			"to":     {strconv.FormatInt(now.UnixMilli(), 10)},
			"filter": {"namespace=" + namespace, "service=billing"},
		}
	}
	prod := []string{"namespace=prod", "service=billing"}

	honest := encodeCallsCursor(serverCursor(t, frozenTo("prod")), prod)
	res, err := c.Calls(context.Background(), CallsInput{Cursor: honest, Window: Window{Since: time.Minute}})
	require.NoError(t, err)
	assert.False(t, res.Restarted, "the server accepts the conditions its cursor froze")
	require.Len(t, reqs, 1)
	assert.Empty(t, reqs[0].Get("from"), "the window of page 1 holds; the one given is not resolved again")

	forged := encodeCallsCursor(serverCursor(t, frozenTo("secret")), prod)
	res, err = c.Calls(context.Background(), CallsInput{Cursor: forged})
	require.NoError(t, err)
	assert.True(t, res.Restarted, "the server rejected the cursor")
	require.Len(t, reqs, 3)
	assert.Equal(t, prod, reqs[1]["filter"])
	assert.Empty(t, reqs[2].Get("cursor"))
	assert.Equal(t, prod, reqs[2]["filter"])
}
