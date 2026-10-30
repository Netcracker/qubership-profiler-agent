package dumps

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
)

const (
	heapPath   = "/diagnostic/ns-1/2026/10/29/10/00/00/app-1-7d9f_1761732000000/20261029T100000.hprof.zip"
	threadPath = "/diagnostic/ns-1/2026/10/29/10/00/00/app-1-7d9f_1761732000000/20261029T100000.td.txt"
)

// received is what the fake upstream saw for one request.
type received struct {
	method      string
	uri         string
	contentType string
	body        []byte
	complete    bool
}

type upstream struct {
	*httptest.Server
	mu       sync.Mutex
	requests []received
	status   func(r *http.Request) int
	// hold, when set, blocks the handler until it is closed.
	hold chan struct{}
	seen chan struct{}
}

func newUpstream(t *testing.T) *upstream {
	u := &upstream{seen: make(chan struct{}, 16)}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.seen <- struct{}{}
		if u.hold != nil {
			<-u.hold
		}
		body, err := io.ReadAll(r.Body)
		u.mu.Lock()
		u.requests = append(u.requests, received{
			method: r.Method, uri: r.RequestURI, contentType: r.Header.Get("Content-Type"),
			body: body, complete: err == nil,
		})
		u.mu.Unlock()
		if err != nil {
			return
		}
		status := http.StatusCreated
		if u.status != nil {
			status = u.status(r)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(u.Close)
	return u
}

func (u *upstream) got() []received {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]received(nil), u.requests...)
}

func (u *upstream) url(t *testing.T) *url.URL {
	parsed, err := url.Parse(u.URL)
	require.NoError(t, err)
	return parsed
}

func baseConfig() Config {
	return Config{
		Endpoint:              "127.0.0.1:0",
		PathPrefix:            "/diagnostic",
		AllowedMethods:        []string{http.MethodPut, http.MethodPost, http.MethodDelete},
		ConnectTimeout:        time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ReadHeaderTimeout:     10 * time.Second,
		MaxBodySize:           64 << 20,
		MaxConcurrentUploads:  4,
		DrainTimeout:          5 * time.Second,
	}
}

func startServer(t *testing.T, cfg Config, policy *rules.Policy, meter ...*sdkmetric.MeterProvider) string {
	t.Helper()
	if policy == nil {
		policy = &rules.Policy{Default: rules.Passthrough}
	}
	m := noop.NewMeterProvider().Meter("test")
	if len(meter) > 0 {
		m = meter[0].Meter("test")
	}
	s, err := New(cfg, policy, zap.NewNop(), m)
	require.NoError(t, err)
	require.NoError(t, s.Start(func(err error) { t.Errorf("serve failed: %v", err) }))
	t.Cleanup(func() { assert.NoError(t, s.Shutdown(context.Background())) })
	return "http://" + s.Addr().String()
}

func randomBytes(t *testing.T, n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

// chunked hides the length of r, so the client sends
// Transfer-Encoding: chunked, as diagtools does for files.
type chunked struct{ r io.Reader }

func (c chunked) Read(p []byte) (int, error) { return c.r.Read(p) }

// countingReader records how much of the body the client managed to send.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

// slowReader sends size bytes in chunks with a pause before each one.
type slowReader struct {
	data  []byte
	chunk int
	pause time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	if len(s.data) == 0 {
		return 0, io.EOF
	}
	time.Sleep(s.pause)
	n := min(s.chunk, len(p), len(s.data))
	copy(p, s.data[:n])
	s.data = s.data[n:]
	return n, nil
}

func do(t *testing.T, method, target string, body io.Reader, contentType string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, target, body)
	require.NoError(t, err)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}

func TestPassthroughForwardsChunkedPutVerbatim(t *testing.T) {
	up := newUpstream(t)
	cfg := baseConfig()
	cfg.Upstream = up.url(t)
	gw := startServer(t, cfg, nil)

	data := randomBytes(t, 1<<20)
	resp := do(t, http.MethodPut, gw+heapPath, chunked{bytes.NewReader(data)}, "application/octet-stream")
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	got := up.got()
	require.Len(t, got, 1)
	assert.Equal(t, http.MethodPut, got[0].method)
	assert.Equal(t, heapPath, got[0].uri)
	assert.Equal(t, "application/octet-stream", got[0].contentType)
	assert.True(t, bytes.Equal(data, got[0].body), "body differs")
}

// Trap 1: the multipart body must be forwarded as raw bytes, not parsed.
func TestPassthroughForwardsMultipartUnparsed(t *testing.T) {
	up := newUpstream(t)
	cfg := baseConfig()
	cfg.Upstream = up.url(t)
	gw := startServer(t, cfg, nil)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	part, err := mw.CreateFormFile("file", "20261029T100000.hprof.zip")
	require.NoError(t, err)
	_, err = part.Write(randomBytes(t, 2<<20))
	require.NoError(t, err)
	require.NoError(t, mw.Close())
	sent := bytes.Clone(buf.Bytes())

	resp := do(t, http.MethodPost, gw+heapPath, chunked{&buf}, mw.FormDataContentType())
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	got := up.got()
	require.Len(t, got, 1)
	assert.Equal(t, mw.FormDataContentType(), got[0].contentType)
	assert.True(t, bytes.Equal(sent, got[0].body), "multipart body differs")
}

func TestPassthroughKeepsUpstreamStatus(t *testing.T) {
	up := newUpstream(t)
	up.status = func(r *http.Request) int {
		if r.Method == http.MethodDelete {
			return http.StatusMethodNotAllowed
		}
		return http.StatusCreated
	}
	cfg := baseConfig()
	cfg.Upstream = up.url(t)
	gw := startServer(t, cfg, nil)

	resp := do(t, http.MethodDelete, gw+heapPath, nil, "")
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, "the client treats 405 on DELETE as fine")
	require.Len(t, up.got(), 1)
}

func TestDiscardDrainsAndAnswers204(t *testing.T) {
	up := newUpstream(t)
	cfg := baseConfig()
	cfg.Upstream = up.url(t)
	gw := startServer(t, cfg, &rules.Policy{Default: rules.Discard})

	body := &countingReader{r: bytes.NewReader(randomBytes(t, 4<<20))}
	resp := do(t, http.MethodPut, gw+heapPath, chunked{body}, "application/octet-stream")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, int64(4<<20), body.n.Load(), "the gateway reads the whole body before answering")

	resp = do(t, http.MethodDelete, gw+heapPath, nil, "")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Empty(t, up.got(), "nothing reaches the upstream in discard")
}

func TestDiscardWorksWithoutUpstream(t *testing.T) {
	gw := startServer(t, baseConfig(), &rules.Policy{Default: rules.Discard})
	resp := do(t, http.MethodPut, gw+threadPath, bytes.NewReader([]byte("dump")), "application/octet-stream")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

// Traps 2 and 3: a body that takes longer than every configured timeout
// still goes through. Only headers have deadlines.
func TestSlowBodyOutlastsTimeouts(t *testing.T) {
	up := newUpstream(t)
	cfg := baseConfig()
	cfg.Upstream = up.url(t)
	cfg.ReadHeaderTimeout = 100 * time.Millisecond
	cfg.ResponseHeaderTimeout = 100 * time.Millisecond
	cfg.ConnectTimeout = 100 * time.Millisecond
	gw := startServer(t, cfg, nil)

	data := randomBytes(t, 64<<10)
	start := time.Now()
	resp := do(t, http.MethodPut, gw+heapPath,
		&slowReader{data: data, chunk: 8 << 10, pause: 60 * time.Millisecond}, "application/octet-stream")
	require.Greater(t, time.Since(start), 400*time.Millisecond, "the body must outlast the timeouts")
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	got := up.got()
	require.Len(t, got, 1)
	assert.True(t, bytes.Equal(data, got[0].body))
}

// Trap 4: without Content-Length the cap is enforced while streaming. The
// upstream request is aborted, the rest is drained, and the client gets 204.
func TestChunkedUploadOverCapIsAbortedAndDiscarded(t *testing.T) {
	up := newUpstream(t)
	cfg := baseConfig()
	cfg.Upstream = up.url(t)
	cfg.MaxBodySize = 64 << 10
	gw := startServer(t, cfg, nil)

	body := &countingReader{r: bytes.NewReader(randomBytes(t, 1<<20))}
	resp := do(t, http.MethodPut, gw+heapPath, chunked{body}, "application/octet-stream")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, int64(1<<20), body.n.Load(), "the rest of the body is drained")

	require.Eventually(t, func() bool { return len(up.got()) == 1 }, 5*time.Second, 10*time.Millisecond)
	got := up.got()[0]
	assert.False(t, got.complete, "the upstream must see an aborted request")
	assert.LessOrEqual(t, len(got.body), 64<<10)
}

func TestDeclaredLengthOverCapNeverDials(t *testing.T) {
	up := newUpstream(t)
	cfg := baseConfig()
	cfg.Upstream = up.url(t)
	cfg.MaxBodySize = 1 << 10
	gw := startServer(t, cfg, nil)

	resp := do(t, http.MethodPut, gw+threadPath, bytes.NewReader(randomBytes(t, 8<<10)), "application/octet-stream")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Empty(t, up.seen, "the upstream is never contacted")
}

func TestPerTypeCap(t *testing.T) {
	up := newUpstream(t)
	cfg := baseConfig()
	cfg.Upstream = up.url(t)
	cfg.MaxBodySizePerType = map[string]int64{rules.DumpTypeThread: 1 << 10}
	gw := startServer(t, cfg, nil)

	resp := do(t, http.MethodPut, gw+threadPath, chunked{bytes.NewReader(randomBytes(t, 4<<10))}, "text/plain")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	resp = do(t, http.MethodPut, gw+heapPath, chunked{bytes.NewReader(randomBytes(t, 4<<10))}, "application/octet-stream")
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
}

func TestConcurrencyCapDiscards(t *testing.T) {
	up := newUpstream(t)
	up.hold = make(chan struct{})
	cfg := baseConfig()
	cfg.Upstream = up.url(t)
	cfg.MaxConcurrentUploads = 1
	gw := startServer(t, cfg, nil)

	first := make(chan *http.Response, 1)
	go func() {
		first <- do(t, http.MethodPut, gw+heapPath, bytes.NewReader([]byte("first")), "application/octet-stream")
	}()
	<-up.seen

	resp := do(t, http.MethodPut, gw+heapPath, bytes.NewReader([]byte("second")), "application/octet-stream")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode, "over the cap the upload is discarded, not queued")

	close(up.hold)
	assert.Equal(t, http.StatusCreated, (<-first).StatusCode)
	assert.Len(t, up.got(), 1)
}

func TestUpstreamDownAnswers502(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	dead := &url.URL{Scheme: "http", Host: ln.Addr().String()}
	require.NoError(t, ln.Close())

	cfg := baseConfig()
	cfg.Upstream = dead
	gw := startServer(t, cfg, nil)

	body := &countingReader{r: bytes.NewReader(randomBytes(t, 256<<10))}
	resp := do(t, http.MethodPut, gw+heapPath, chunked{body}, "application/octet-stream")
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode, "the client keeps the file and retries")
}

func TestRulesSelectMode(t *testing.T) {
	up := newUpstream(t)
	cfg := baseConfig()
	cfg.Upstream = up.url(t)
	policy := &rules.Policy{Default: rules.Passthrough, Inline: rules.Set{
		{Match: rules.Match{Namespace: "ns-off"}, Mode: rules.Discard},
		{Match: rules.Match{Pod: "debug-*"}, Mode: rules.Discard},
		{Match: rules.Match{DumpType: []string{rules.DumpTypeThread}}, Mode: rules.Discard},
	}}
	gw := startServer(t, cfg, policy)

	for path, want := range map[string]int{
		heapPath:   http.StatusCreated,
		threadPath: http.StatusNoContent,
		"/diagnostic/ns-off/2026/10/29/10/00/00/app-1/20261029T100000.hprof.zip":   http.StatusNoContent,
		"/diagnostic/ns-1/2026/10/29/10/00/00/debug-1_1761732000000/gc.log":        http.StatusNoContent,
		"/diagnostic/ns-1/2026/10/29/10/00/00/app-1_1761732000000/gc.log.0":        http.StatusCreated,
		"/diagnostic/ns-1/2026/10/29/10/00/00/app-1_1761732000000/hs_err_pid1.log": http.StatusCreated,
	} {
		resp := do(t, http.MethodPut, gw+path, bytes.NewReader([]byte("x")), "application/octet-stream")
		assert.Equal(t, want, resp.StatusCode, path)
	}
}

func TestPrefixAndMethods(t *testing.T) {
	up := newUpstream(t)
	cfg := baseConfig()
	cfg.Upstream = up.url(t)
	gw := startServer(t, cfg, nil)

	resp := do(t, http.MethodGet, gw+heapPath, nil, "")
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	assert.Equal(t, "PUT, POST, DELETE", resp.Header.Get("Allow"))

	resp = do(t, http.MethodPut, gw+"/diagnosticx/a", bytes.NewReader([]byte("x")), "")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Empty(t, up.got())
}

func TestParsePath(t *testing.T) {
	a := parsePath(heapPath, "/diagnostic")
	assert.Equal(t, rules.Attrs{Namespace: "ns-1", Pod: "app-1-7d9f", DumpType: rules.DumpTypeHeap}, a)
	assert.Equal(t, rules.DumpTypeThread, parsePath(threadPath, "/diagnostic/").DumpType)
	assert.Equal(t, rules.Attrs{}, parsePath("/diagnostic/ns-1/file", "/diagnostic"))

	assert.Equal(t, rules.DumpTypeHeap, dumpType("20261029T100000.hprof"))
	assert.Equal(t, rules.DumpTypeGC, dumpType("gc.log"))
	assert.Equal(t, rules.DumpTypeGC, dumpType("gc.log.3"))
	assert.Equal(t, rules.DumpTypeOther, dumpType("core.1234"))
	assert.Equal(t, rules.DumpTypeOther, dumpType("gc.logger"))
}

func TestMetrics(t *testing.T) {
	up := newUpstream(t)
	cfg := baseConfig()
	cfg.Upstream = up.url(t)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	policy := &rules.Policy{Default: rules.Passthrough, Inline: rules.Set{
		{Match: rules.Match{DumpType: []string{rules.DumpTypeThread}}, Mode: rules.Discard},
	}}
	gw := startServer(t, cfg, policy, provider)

	do(t, http.MethodPut, gw+heapPath, chunked{bytes.NewReader(make([]byte, 1000))}, "application/octet-stream")
	do(t, http.MethodPut, gw+threadPath, bytes.NewReader(make([]byte, 300)), "text/plain")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	sums := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if s, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range s.DataPoints {
					key := m.Name
					if v, ok := dp.Attributes.Value("outcome"); ok {
						key += "/" + v.AsString()
					}
					sums[key] += dp.Value
				}
			}
		}
	}
	assert.Equal(t, int64(1000), sums["profiler_gateway_upload_bytes_total/forwarded"])
	assert.Equal(t, int64(300), sums["profiler_gateway_upload_bytes_total/discarded"])
	assert.Equal(t, int64(1000), sums["profiler_gateway_bytes_forwarded_total"])
	assert.Equal(t, int64(300), sums["profiler_gateway_bytes_discarded_total"])
	assert.Equal(t, int64(0), sums["profiler_gateway_uploads_in_flight"])
}
