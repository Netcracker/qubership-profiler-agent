package wire

import (
	"bytes"

	"github.com/pkg/errors"
)

// PhraseBuffer mirrors the agent's PhraseOutputStream
// (proto-definition/.../transport/PhraseOutputStream.java): phrase bytes
// accumulate in a bounded buffer, ClosePhrase marks a phrase boundary, and the
// buffer emits `[fixed-int length][bytes]` frames covering only closed phrases
// — when nearly full (within 200 bytes of capacity), when an incoming write
// would overflow, or on Flush. Bytes written after the last ClosePhrase stay
// buffered until their phrase closes.
//
// The dictionary, suspend, and params streams are phrase-framed; everything
// else is raw (virtual-dumper.md §2.2).
type PhraseBuffer struct {
	max    int
	buf    []byte
	closed int
	out    func(frame []byte) error
}

// NewPhraseBuffer builds a phrase buffer of the given capacity (normally
// MaxPhraseSize) that hands each emitted frame to out.
func NewPhraseBuffer(max int, out func(frame []byte) error) *PhraseBuffer {
	return &PhraseBuffer{max: max, out: out}
}

// Write appends phrase bytes, emitting closed phrases first when b would not
// fit (PhraseOutputStream.ensureCapacity). A single phrase larger than the
// buffer is an error, exactly like the agent's ProfilerProtocolException.
func (p *PhraseBuffer) Write(b []byte) error {
	if len(b) > p.max {
		return errors.Errorf("phrase write of %d bytes exceeds the %d-byte buffer", len(b), p.max)
	}
	if len(p.buf)+len(b) >= p.max {
		if err := p.emit(); err != nil {
			return err
		}
	}
	if len(p.buf)+len(b) >= p.max {
		return errors.Errorf("phrase of %d+%d uncommitted bytes exceeds the %d-byte buffer",
			len(p.buf), len(b), p.max)
	}
	p.buf = append(p.buf, b...)
	return nil
}

// ClosePhrase marks everything buffered so far as one committed phrase and
// emits when the buffer is nearly full (PhraseOutputStream.writePhrase).
func (p *PhraseBuffer) ClosePhrase() error {
	p.closed = len(p.buf)
	if len(p.buf) < p.max-200 {
		return nil
	}
	return p.emit()
}

// Flush force-emits the committed phrases; uncommitted bytes stay buffered.
func (p *PhraseBuffer) Flush() error {
	return p.emit()
}

func (p *PhraseBuffer) emit() error {
	if p.closed == 0 {
		return nil
	}
	frame := &bytes.Buffer{}
	PutFixedInt(frame, uint32(p.closed))
	frame.Write(p.buf[:p.closed])
	rest := len(p.buf) - p.closed
	copy(p.buf, p.buf[p.closed:])
	p.buf = p.buf[:rest]
	p.closed = 0
	return p.out(frame.Bytes())
}
