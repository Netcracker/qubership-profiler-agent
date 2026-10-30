package dumps

import (
	"errors"
	"io"
	"sync"
)

var (
	errOverCap  = errors.New("upload exceeds the size cap")
	errDetached = errors.New("upload body already drained")
)

// capBody wraps the client's request body on its way to the upstream.
//
// It counts bytes and fails with errOverCap once the count passes the
// limit, which aborts the upstream request. Close is a no-op, so the body
// can still be drained after the transport gives up on it. A mutex makes
// drain safe while the transport's writer goroutine is still reading.
type capBody struct {
	mu        sync.Mutex
	r         io.Reader
	limit     int64
	n         int64
	eof       bool
	overCap   bool
	detached  bool
	clientErr error
}

func newCapBody(r io.Reader, limit int64) *capBody {
	return &capBody{r: r, limit: limit}
}

func (b *capBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.detached:
		return 0, errDetached
	case b.overCap:
		return 0, errOverCap
	case b.eof:
		return 0, io.EOF
	}
	n, err := b.r.Read(p)
	b.n += int64(n)
	if b.n > b.limit {
		b.overCap = true
		return 0, errOverCap
	}
	switch {
	case errors.Is(err, io.EOF):
		b.eof = true
	case err != nil:
		b.clientErr = err
	}
	return n, err
}

// Close leaves the client body open; the HTTP server closes it.
func (b *capBody) Close() error { return nil }

// drain reads whatever the client has not sent yet and discards it. Later
// reads by the transport fail. It returns the number of bytes drained.
func (b *capBody) drain() (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.detached = true
	if b.eof || b.clientErr != nil {
		return 0, b.clientErr
	}
	n, err := io.Copy(io.Discard, b.r)
	b.n += n
	if err != nil {
		b.clientErr = err
	} else {
		b.eof = true
	}
	return n, err
}

type bodyState struct {
	total     int64
	overCap   bool
	clientErr error
}

func (b *capBody) state() bodyState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bodyState{total: b.n, overCap: b.overCap, clientErr: b.clientErr}
}
