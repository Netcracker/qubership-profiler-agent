// Package insight holds the task-level operations that agents run against
// the profiler's read API: list services, find calls, and (in later tasks)
// explain a call and compute statistics. The `profiler` CLI and the MCP
// server are thin front ends over it, so both return the same results.
//
// Every operation talks HTTP to the query service's /api/v1 (02-read-contract
// §2). The package knows nothing about flags, MCP, or output formats.
package insight

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// BasePath is the prefix of every read API route the client calls.
const BasePath = "/api/v1"

// maxRetryWait caps how long the client honors a Retry-After before its one
// retry of a read_budget_exhausted answer (section 6.6 of the agent-access
// plan); a longer hint fails at once so an agent is not left waiting.
const maxRetryWait = 10 * time.Second

// Client calls the query service. It is safe for concurrent use.
type Client struct {
	baseURL    string
	http       *http.Client
	credential CredentialSource
	authorize  Authorizer
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient replaces the default http.Client.
func WithHTTPClient(c *http.Client) Option {
	return func(cl *Client) { cl.http = c }
}

// WithCredentialSource attaches a credential to every request. The default
// reads the credential a caller put in the context with WithCredential.
func WithCredentialSource(s CredentialSource) Option {
	return func(cl *Client) { cl.credential = s }
}

// WithAuthorizer sets the check every operation runs on the namespaces it
// reads. The default allows every namespace.
func WithAuthorizer(a Authorizer) Option {
	return func(cl *Client) { cl.authorize = a }
}

// New returns a client for the query service at baseURL, such as
// http://localhost:8080. A trailing BasePath is accepted and dropped.
func New(baseURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.Errorf("profiler URL %q is not an http or https URL", baseURL)
	}
	base := strings.TrimSuffix(strings.TrimSuffix(u.String(), "/"), BasePath)
	c := &Client{
		baseURL:    base,
		http:       &http.Client{Timeout: 60 * time.Second},
		credential: ContextCredential{},
		authorize:  AllowAll,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// getJSON fetches BasePath+path and decodes a 200 body into out. A problem
// response becomes an *APIError. A read_budget_exhausted answer is retried
// once after its Retry-After.
func (c *Client) getJSON(ctx context.Context, path string, query url.Values, out any) error {
	err := c.getOnce(ctx, path, query, out)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != CodeReadBudgetExhausted || apiErr.RetryAfter > maxRetryWait {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(apiErr.RetryAfter):
	}
	return c.getOnce(ctx, path, query, out)
}

func (c *Client) getOnce(ctx context.Context, path string, query url.Values, out any) error {
	target := c.baseURL + BasePath + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return errors.Wrap(err, "build request")
	}
	req.Header.Set("Accept", "application/json")
	credential, err := c.credential.Credential(ctx)
	if err != nil {
		return errors.Wrap(err, "get credential")
	}
	if credential != "" {
		req.Header.Set("Authorization", credential)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return errors.Wrapf(err, "GET %s", BasePath+path)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "read the response of GET %s", BasePath+path)
	}
	if resp.StatusCode != http.StatusOK {
		return decodeProblem(resp, body)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return errors.Wrapf(err, "decode the response of GET %s", BasePath+path)
	}
	return nil
}

func decodeProblem(resp *http.Response, body []byte) error {
	e := &APIError{Status: resp.StatusCode}
	if err := json.Unmarshal(body, e); err != nil || e.Title == "" && e.Code == "" {
		// Not a Problem Details body: a proxy or an older build answered.
		e.Detail = strings.TrimSpace(string(body))
		if len(e.Detail) > 200 {
			e.Detail = e.Detail[:200] + "..."
		}
	}
	e.Status = resp.StatusCode
	if secs, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && secs >= 0 {
		e.RetryAfter = time.Duration(secs) * time.Second
	}
	return e
}
