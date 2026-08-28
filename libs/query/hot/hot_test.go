package hot

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingReader is a body that reports how many bytes were actually pulled off
// it, so a test can prove the budgeted reader never reads past its cap.
type countingReader struct {
	data []byte
	pos  int
	read int
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	r.read += n
	return n, nil
}

// errAtEndReader serves data then fails with endErr (not io.EOF) once
// exhausted — a transport error surfacing on the boundary probe read.
type errAtEndReader struct {
	data   []byte
	pos    int
	endErr error
}

func (r *errAtEndReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, r.endErr
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

var tracePK = model.PK{
	PodNamespace: "ns", PodService: "svc", PodName: "pod", RestartTimeMs: 1000,
	TraceFileIndex: 1, BufferOffset: 2, RecordIndex: 0,
}

// TestTraceBudgetedBody pins the §7.5 body reader on the blob endpoint: a
// declared size is charged up front and owned by the returned lease; a body
// past the cap is refused on the headers alone; a chunked body is read in
// pre-reserved steps and still lands under the lease.
func TestTraceBudgetedBody(t *testing.T) {
	blob := strings.Repeat("B", 4096)

	t.Run("content-length charged and owned", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", fmt.Sprint(len(blob)))
			_, _ = w.Write([]byte(blob))
		}))
		defer srv.Close()
		b := budget.New(1<<20, time.Second, budget.Hooks{})
		c := NewClient(time.Second, b)
		got, lease, found, err := c.Trace(context.Background(), srv.URL, tracePK)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, blob, string(got))
		assert.Equal(t, int64(len(blob)), lease.Held(), "the lease owns exactly the blob")
		lease.Release()
		assert.Equal(t, int64(0), b.Used())
	})

	t.Run("declared size past the cap refused on headers", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", fmt.Sprint(int64(maxHotBlobBytes)+1))
			// The handler never gets to stream the body: the client must bail
			// out on the headers.
			_, _ = w.Write([]byte("x"))
		}))
		defer srv.Close()
		b := budget.New(1<<20, time.Second, budget.Hooks{})
		c := NewClient(time.Second, b)
		_, _, _, err := c.Trace(context.Background(), srv.URL, tracePK)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds")
		assert.Equal(t, int64(0), b.Used())
	})

	t.Run("chunked body reserved before read", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			// No Content-Length: the server flushes chunks.
			fl := w.(http.Flusher)
			for i := 0; i < 3; i++ {
				_, _ = w.Write([]byte(blob))
				fl.Flush()
			}
		}))
		defer srv.Close()
		b := budget.New(64<<20, time.Second, budget.Hooks{})
		c := NewClient(time.Second, b)
		got, lease, found, err := c.Trace(context.Background(), srv.URL, tracePK)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, strings.Repeat(blob, 3), string(got))
		assert.Equal(t, int64(3*len(blob)), lease.Held())
		lease.Release()
		assert.Equal(t, int64(0), b.Used())
	})

	t.Run("chunked denial under a saturated budget", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fl := w.(http.Flusher)
			_, _ = w.Write([]byte(blob))
			fl.Flush()
		}))
		defer srv.Close()
		b := budget.New(1<<20, 30*time.Millisecond, budget.Hooks{})
		blocker, err := b.Acquire(context.Background(), 1<<20)
		require.NoError(t, err)
		defer blocker.Release()
		c := NewClient(time.Second, b)
		_, _, _, err = c.Trace(context.Background(), srv.URL, tracePK)
		require.Error(t, err)
		assert.ErrorIs(t, err, budget.ErrExhausted)
		blocker.Release()
		assert.Equal(t, int64(0), b.Used())
	})
}

// TestChunkedBodyCapBoundary pins the §7.5 chunked reader at the cap. A
// non-chunk-multiple cap exercises the clamped final read where the boundary
// lived: a body over the cap is refused having pulled at most one probe byte
// past it, and a body exactly at the cap is served whole (matching the
// Content-Length branch, which accepts cl == maxBytes).
func TestChunkedBodyCapBoundary(t *testing.T) {
	capBytes := int64(bodyChunkBytes + bodyChunkBytes/2) // 1.5 chunks, not a multiple

	t.Run("over the cap is refused, at most one probe byte past it", func(t *testing.T) {
		cr := &countingReader{data: make([]byte, capBytes*4)}
		resp := &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Body: io.NopCloser(cr)}
		b := budget.New(64<<20, time.Second, budget.Hooks{})
		c := NewClient(time.Second, b)
		_, _, err := c.readBudgetedBody(context.Background(), resp, capBytes)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds")
		assert.LessOrEqual(t, int64(cr.read), capBytes+1, "reads at most one probe byte past the cap")
		assert.Equal(t, int64(0), b.Used(), "the lease is released on refusal")
	})

	t.Run("exactly at the cap is accepted", func(t *testing.T) {
		cr := &countingReader{data: make([]byte, capBytes)}
		resp := &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Body: io.NopCloser(cr)}
		b := budget.New(64<<20, time.Second, budget.Hooks{})
		c := NewClient(time.Second, b)
		body, lease, err := c.readBudgetedBody(context.Background(), resp, capBytes)
		require.NoError(t, err)
		assert.Equal(t, int(capBytes), len(body), "a body exactly at the cap is served whole")
		assert.Equal(t, capBytes, lease.Held())
		lease.Release()
		assert.Equal(t, int64(0), b.Used())
	})

	t.Run("transport error on the boundary probe is surfaced", func(t *testing.T) {
		// The body fills exactly to the cap, then the probe read fails with a
		// non-EOF transport error: the body is unconfirmed and must not be
		// served as complete.
		boom := fmt.Errorf("transport reset at the cap")
		r := &errAtEndReader{data: make([]byte, capBytes), endErr: boom}
		resp := &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Body: io.NopCloser(r)}
		b := budget.New(64<<20, time.Second, budget.Hooks{})
		c := NewClient(time.Second, b)
		_, _, err := c.readBudgetedBody(context.Background(), resp, capBytes)
		require.Error(t, err)
		assert.ErrorIs(t, err, boom, "an unconfirmed body is not accepted as complete")
		assert.Equal(t, int64(0), b.Used(), "the lease is released on the boundary error")
	})
}

// TestCallsGatesDecodePeak pins the §7.5 decode reservation: /calls charges the
// raw body plus a conservative multiple BEFORE unmarshaling, so a budget that
// admits the raw body but not the decode peak sheds the request with a budget
// denial instead of letting json.Unmarshal allocate past the budget.
func TestCallsGatesDecodePeak(t *testing.T) {
	body := `{"calls":[{"pk":{"pod_namespace":"ns","pod_service":"svc","pod_name":"pod",` +
		`"restart_time_ms":1000,"trace_file_index":1,"buffer_offset":2,"record_index":0},` +
		`"ts_ms":1234,"duration_ms":10,"method":"com.example.M.handle"}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	// Fits the raw body but not raw*(1+hotDecodeReserveFactor): the decode-peak
	// reservation must trip, where the old body-only charge would have passed.
	raw := int64(len(body))
	b := budget.New(raw+raw*hotDecodeReserveFactor-1, 50*time.Millisecond, budget.Hooks{})
	c := NewClient(time.Second, b)
	_, _, err := c.Calls(context.Background(), srv.URL, model.CallsQuery{FromMs: 0, ToMs: 2000}, nil, 10)
	require.Error(t, err)
	assert.ErrorIs(t, err, budget.ErrNeverFits)
	assert.Equal(t, int64(0), b.Used(), "the lease is released on the denied decode reservation")
}

// TestCallsLeaseOwnsDecodedRows pins the hot list path: the lease returned
// with the rows is reconciled to the rows' accounting footprint, not the raw
// body size.
func TestCallsLeaseOwnsDecodedRows(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"calls":[{"pk":{"pod_namespace":"ns","pod_service":"svc","pod_name":"pod",` +
			`"restart_time_ms":1000,"trace_file_index":1,"buffer_offset":2,"record_index":0},` +
			`"ts_ms":1234,"duration_ms":10,"method":"com.example.M.handle"}]}`))
	}))
	defer srv.Close()
	b := budget.New(1<<20, time.Second, budget.Hooks{})
	c := NewClient(time.Second, b)
	rows, lease, err := c.Calls(context.Background(), srv.URL, model.CallsQuery{FromMs: 0, ToMs: 2000}, nil, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	var want int64
	for i := range rows {
		want += model.RowFootprint(&rows[i])
	}
	assert.Equal(t, want, lease.Held(), "the lease is reconciled to the decoded rows")
	lease.Release()
	assert.Equal(t, int64(0), b.Used())
}
