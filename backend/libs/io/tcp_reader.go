package io

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"unicode/utf16"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
)

type (
	TcpReader struct {
		reader *bufio.Reader
		err    error
		eof    bool
		pos    uint64
		debug  bool
	}
)

func (tr *TcpReader) EOF() bool {
	return tr.eof
}

func (tr *TcpReader) Pos() uint64 {
	// is it possible to overflow it in our agent<->collector communication?
	return tr.pos
}

func (tr *TcpReader) Done() {
	tr.eof = true
}


func (tr *TcpReader) SetDebug(debug bool) {
	tr.debug = debug
}

// Buffered reports how many bytes are readable without touching the underlying
// reader. Together with Peek it backs the agent client's opportunistic ack
// drain (DefaultCollectorClient.validateWriteDataAcks(false), which polls
// in.available() before every RCV_DATA).
func (tr *TcpReader) Buffered() int {
	return tr.reader.Buffered()
}

// Peek returns the next n bytes without consuming them, filling from the
// underlying reader when the buffer holds fewer than n. With an immediate read
// deadline on the underlying connection this is a non-blocking readability
// probe: a timeout error means nothing is readable right now.
func (tr *TcpReader) Peek(n int) ([]byte, error) {
	return tr.reader.Peek(n)
}

func PrepareTcpReader(reader io.Reader) *TcpReader {
	buffered := bufio.NewReaderSize(reader, 4096)
	return &TcpReader{buffered, nil, false, 0, false}
}

// simple parsing

func (tr *TcpReader) ReadFixedByte(ctx context.Context) (byte, error) {
	var op byte
	tr.read(ctx, &op)
	return op, tr.err
}

func (tr *TcpReader) ReadFixedInt(ctx context.Context) (int, error) {
	var op uint32
	tr.read(ctx, &op)
	return int(op), tr.err
}
func (tr *TcpReader) ReadFixedLong(ctx context.Context) (uint64, error) {
	var op uint64
	tr.read(ctx, &op)
	return op, tr.err
}
func (tr *TcpReader) ReadUuid(ctx context.Context) (common.Uuid, error) {
	data := make([]byte, 16)
	tr.read(ctx, data)
	o := [16]byte{}
	for i := 0; i < 16; i++ {
		o[i] = data[i]
	}
	return common.ToUuid(o), tr.err
}

func (tr *TcpReader) ReadFixedString(ctx context.Context) (string, error) {
	var length = tr.readLen(ctx)
	if tr.err != nil {
		return "", tr.err
	}
	// The agent's FieldIOReader.Field rejects any field longer than
	// DATA_BUFFER_SIZE, so a length past that ceiling is a malformed or hostile
	// client. Refuse it instead of make()-ing up to 4 GiB from one wire number;
	// the caller replies ACK_ERROR_MAGIC and closes (06 §2, №13).
	if length > model.DataBufferSize {
		tr.err = fmt.Errorf("fixed-string length %d exceeds max %d at pos %d",
			length, model.DataBufferSize, tr.pos)
		tr.eof = true
		return "", tr.err
	}
	data := make([]byte, length)
	tr.read(ctx, data)
	return string(data), tr.err
}

func (tr *TcpReader) readLen(ctx context.Context) uint32 {
	var op uint32
	tr.read(ctx, &op)
	return op
}

func (tr *TcpReader) readChar(ctx context.Context) string {
	var op uint16
	tr.read(ctx, &op)
	// One UTF-16 code unit, decoded like the agent's DataInputStreamEx.readChar.
	// A lone surrogate half decodes to U+FFFD here; callers that read whole
	// strings collect the units and decode the run so pairs reassemble.
	return string(utf16.Decode([]uint16{op}))
}

func (tr *TcpReader) read(ctx context.Context, o interface{}) {
	tr.err = binary.Read(tr.reader, binary.BigEndian, o)
	if tr.err == io.EOF {
		tr.eof = true
	}
	if tr.debug {
		if binary.Size(o) > 1 {
			log.Trace(ctx, "<- #%5d, got %d bytes: %s", tr.pos, binary.Size(o), common.AsHex(asBytes(o), 30))
		} else {
			log.Debug(ctx, "<- #%5d, got %d bytes: %s", tr.pos, binary.Size(o), common.AsHex(asBytes(o), 30))
		}
	}
	if tr.err != nil {
		if IsExpectedDisconnect(tr.err) {
			log.Debug(ctx, "connection closed at pos # %d: %v", tr.pos, tr.err)
		} else {
			log.Error(ctx, tr.err, "could not read at pos # %d", tr.pos)
		}
	}
	tr.pos += uint64(binary.Size(o))
}

func asBytes(o interface{}) []byte {
	var b strings.Builder
	_ = binary.Write(&b, binary.BigEndian, o)
	return []byte(b.String())
}
