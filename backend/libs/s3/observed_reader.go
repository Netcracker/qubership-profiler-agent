package s3

import (
	"context"
	"io"
	"sync"
	"time"
)

// ObservedReaderAt wraps the ranged reads of one opened S3 object and records
// each read that reaches S3 as an [OperationGet] request with no objects,
// since opening the object already counted it. It is safe for concurrent use
// when the wrapped reader is.
//
// A read at an offset outside [0, size) records nothing, because the minio
// client answers it with io.EOF without a request. After the first failed
// read, and after Close, no read records anything: the minio client keeps
// the first error and returns it from every later read without a request.
type ObservedReaderAt struct {
	ctx  context.Context
	r    io.ReaderAt
	size int64

	mu      sync.Mutex
	stopped bool
}

// NewObservedReaderAt wraps r, an object of size bytes opened under ctx.
// ctx decides whether a failed read was cancelled by the caller; see
// [ObserveResult].
func NewObservedReaderAt(ctx context.Context, r io.ReaderAt, size int64) *ObservedReaderAt {
	return &ObservedReaderAt{ctx: ctx, r: r, size: size}
}

// ReadAt implements [io.ReaderAt].
func (o *ObservedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= o.size || o.isStopped() {
		return o.r.ReadAt(p, off)
	}
	start := time.Now()
	n, err := o.r.ReadAt(p, off)
	if err == nil || err == io.EOF {
		// The last range of an object ends in io.EOF.
		ObserveResult(o.ctx, OperationGet, start, 0, nil)
		return n, err
	}
	if o.stop() {
		ObserveResult(o.ctx, OperationGet, start, 0, err)
	}
	return n, err
}

// Close stops recording. It does not close the wrapped reader.
func (o *ObservedReaderAt) Close() {
	_ = o.stop()
}

func (o *ObservedReaderAt) isStopped() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.stopped
}

// stop reports whether this call stopped the reader, so that of two
// concurrent reads failing with the same error only one records it.
func (o *ObservedReaderAt) stop() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	first := !o.stopped
	o.stopped = true
	return first
}
