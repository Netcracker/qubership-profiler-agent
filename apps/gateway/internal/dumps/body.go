package dumps

import (
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

var (
	errOverCap  = errors.New("upload exceeds the size cap")
	errDetached = errors.New("upload body already drained")
)

// capBody wraps the client's request body on its way to the upstream.
//
// It counts the bytes it receives and the bytes it passes to the transport,
// and fails with errOverCap once the received count passes the limit, which
// aborts the upstream request. Close is a no-op, so the body
// can still be drained after the transport gives up on it. A mutex makes
// drain safe while the transport's writer goroutine is still reading.
type capBody struct {
	mu        sync.Mutex
	r         io.Reader
	limit     int64
	n         int64
	sent      int64
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
	b.sent += int64(n)
	return n, err
}

// Close leaves the client body open; the HTTP server closes it.
func (b *capBody) Close() error { return nil }

// drain reads whatever the client has not sent yet and discards it. Later
// reads by the transport fail. A body whose full size passes the limit is
// marked over the cap, also when the upstream answered before the
// transport read that far. It returns the number of bytes drained.
func (b *capBody) drain() (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.detached = true
	if b.eof || b.clientErr != nil {
		return 0, b.clientErr
	}
	n, err := io.Copy(io.Discard, b.r)
	b.n += n
	if b.n > b.limit {
		b.overCap = true
	}
	if err != nil {
		b.clientErr = err
	} else {
		b.eof = true
	}
	return n, err
}

type bodyState struct {
	total     int64
	sent      int64
	overCap   bool
	clientErr error
}

func (b *capBody) state() bodyState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bodyState{total: b.n, sent: b.sent, overCap: b.overCap, clientErr: b.clientErr}
}

// idleBody renews a read deadline on the client connection before each
// read, so a client that stops sending fails the read instead of holding
// an upload slot. A body that keeps moving has no overall deadline. At EOF
// the deadline is cleared: the server's background read starts then and
// must not time out while the upstream answers.
type idleBody struct {
	io.ReadCloser
	rc      *http.ResponseController
	timeout time.Duration
}

func (b *idleBody) Read(p []byte) (int, error) {
	_ = b.rc.SetReadDeadline(time.Now().Add(b.timeout))
	n, err := b.ReadCloser.Read(p)
	if errors.Is(err, io.EOF) {
		_ = b.rc.SetReadDeadline(time.Time{})
	}
	return n, err
}

// writeIdleConn bounds each write to the upstream, like nginx
// proxy_send_timeout. An upstream that stops reading fails the write, and
// the transport aborts the request.
type writeIdleConn struct {
	net.Conn
	timeout time.Duration
}

func (c *writeIdleConn) Write(p []byte) (int, error) {
	if err := c.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}
