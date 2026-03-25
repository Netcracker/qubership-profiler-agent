package io

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type (
	TcpReader struct {
		reader io.Reader
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
	var op int16
	tr.read(ctx, &op)
	//return string(op)
	return string(rune(op))
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
		log.Error(ctx, tr.err, "could not read at pos # %d", tr.pos)
	}
	tr.pos += uint64(binary.Size(o))
}

func asBytes(o interface{}) []byte {
	var b strings.Builder
	_ = binary.Write(&b, binary.BigEndian, o)
	return []byte(b.String())
}
