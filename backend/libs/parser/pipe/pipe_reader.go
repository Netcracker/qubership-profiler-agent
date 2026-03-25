package pipe

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"io"
	"slices"
	"strings"
)

type (
	// PipeReader general structure to provide similar helpers to parse protocol, but from pipe
	PipeReader struct {
		reader     io.Reader
		isDone     bool
		chars      int64        // chars already be read
		needBuffer bool         // required for parsing traces
		buffer     bytes.Buffer // internal buffer to keep the latest n bytes
	}
)

func NewPipeReader(reader io.Reader, needBuffer bool) *PipeReader {
	var buf bytes.Buffer
	return &PipeReader{reader, false, 0, needBuffer, buf}
}

func (b *PipeReader) EOF() bool {
	return b.isDone
}

func (b *PipeReader) Done() {
	b.isDone = true
}

func (b *PipeReader) Next(i ...uint32) bool {
	if len(i) == 1 {
		b.chars += int64(i[0])
	} else {
		b.chars++
	}
	return b.EOF()
}

func (b *PipeReader) Position() int64 {
	return b.chars
}

func (b *PipeReader) GetAndEmptyBuffer() []byte {
	copyBuf := slices.Clone(b.buffer.Bytes())
	b.buffer.Reset()
	return copyBuf
}

// simple parsing
// NB: change of signature -- here we are returning error, should not silently ignore problems
// TODO align signature of all parsers to one common interface

func (b *PipeReader) ReadFixedByte(ctx context.Context) (byte, error) {
	var op byte
	err := b.read(ctx, &op)
	b.Next()
	return op, err
}

func (b *PipeReader) ReadFixedInt(ctx context.Context) (int, error) {
	var op uint32
	err := b.read(ctx, &op)
	b.Next(4)
	return int(op), err
}
func (b *PipeReader) ReadFixedLong(ctx context.Context) (uint64, error) {
	var op uint64
	err := b.read(ctx, &op)
	b.Next(8)
	return op, err
}
func (b *PipeReader) ReadUuid(ctx context.Context) (common.Uuid, error) {
	data := make([]byte, 16)
	err := b.read(ctx, data)
	o := [16]byte{}
	for i := 0; i < 16; i++ {
		o[i] = data[i]
	}
	b.Next(16)
	return common.ToUuid(o), err
}

func (b *PipeReader) ReadFixedString(ctx context.Context) (string, error) {
	length, err := b.readLen(ctx)
	// TODO safety check (warning and limit for length > 2mb ?)
	//  * reason: must be err, because we have limiter (4kb for large objects) on the profiler agent side
	data := make([]byte, length)
	err = b.read(ctx, data)
	b.Next(length)
	return string(data), err
}

func (b *PipeReader) ReadVarInt(ctx context.Context) (int, error) {
	read := func() (int, error) {
		var x uint8
		err := b.read(ctx, &x)
		b.Next()
		if err != nil {
			return -1, err
		}
		return int(x), nil
	}
	var res int

	x, err := read()
	if err != nil {
		return -1, err
	}
	if x == -1 {
		return -1, nil
	} else if (x & 0x80) == 0 {
		return x, nil
	}
	res = x & ^0x80

	x, err = read()
	if err != nil {
		return -1, err
	}
	res |= x << 7
	if (res & (0x80 << 7)) == 0 {
		return res, nil
	}
	res &= ^(0x80 << 7)

	x, err = read()
	if err != nil {
		return -1, err
	}
	res = res | x<<14
	if (res & (0x80 << 14)) == 0 {
		return res, nil
	}
	res &= ^(0x80 << 14)

	x, err = read()
	if err != nil {
		return -1, err
	}
	res |= x << 21
	if (res & (0x80 << 21)) == 0 {
		return res, nil
	}
	res &= ^(0x80 << 21)

	x, err = read()
	if err != nil {
		return -1, err
	}
	res |= x << 28
	return res, nil
}

func (b *PipeReader) ReadVarLong(ctx context.Context) (int64, error) {
	read := func() (int64, error) {
		var x byte
		err := b.read(ctx, &x)
		b.Next()
		if err != nil {
			b.isDone = true
			return -1, err
		}
		return int64(x), nil
	}
	var res int64

	x, err := read()
	if err != nil {
		b.isDone = true
		return -1, err
	}
	if x == -1 {
		return -1, nil
	} else if (x & 0x80) == 0 {
		return x, nil
	}
	res = x & ^0x80 // 0..6

	x, err = read()
	if err != nil {
		b.isDone = true
		return -1, err
	}
	res |= x << 7
	if (res & (0x80 << 7)) == 0 {
		return res, nil
	}
	res &= ^(0x80 << 7) // 7..13

	x, err = read()
	if err != nil {
		b.isDone = true
		return -1, err
	}
	res = res | x<<14
	if (res & (0x80 << 14)) == 0 {
		return res, nil
	}
	res &= ^(0x80 << 14) // 14..20

	x, err = read()
	if err != nil {
		b.isDone = true
		return -1, err
	}
	res |= x << 21
	if (res & (0x80 << 21)) == 0 {
		return res, nil
	}
	res &= ^(0x80 << 21) // 21..28

	x, err = read()
	if err != nil {
		return -1, err
	}
	if (x & 0x80) == 0 {
		return (x << 28) | res, nil
	}
	resLong := ((x & 0x7f) << 28) | res
	i, err := b.ReadVarInt(ctx)
	if err != nil {
		b.isDone = true
		return -1, err
	}
	result := (int64(i) << 35) | resLong
	return result, nil
}

func (b *PipeReader) ReadVarIntZigZag(ctx context.Context) (int, error) {
	res, err := b.ReadVarInt(ctx)
	if err != nil {
		b.isDone = true
		return -1, err
	}
	res = (res >> 1) ^ (-(res & 1))
	return res, nil
}

func (b *PipeReader) ReadVarString(ctx context.Context) (int, int, string) {
	before := b.chars
	maxLength := 10 * 1024 * 1024
	length, err := b.ReadVarInt(ctx)
	if err != nil {
		b.isDone = true
		return -1, 0, ""
	}
	if length > maxLength {
		// Exception: "Expecting string of max length %maxLength, got %length chars at %position"
		b.isDone = true
		return -1, 0, ""
	}
	var sb strings.Builder
	var char string
	for i := 0; i < length; i++ {
		char, err = b.readChar(ctx)
		if err != nil {
			b.isDone = true
			return -1, int(b.chars - before), sb.String()
		}
		sb.WriteString(char)
	}
	// length of returned string
	// actual number of bytes which were read (including varint in the beginning)
	// actual string
	return length, int(b.chars - before), sb.String()
}

func (b *PipeReader) readLen(ctx context.Context) (uint32, error) {
	var op uint32
	err := b.read(ctx, &op)
	b.Next(4)
	return op, err
}

func (b *PipeReader) readChar(ctx context.Context) (string, error) {
	var op int16
	err := b.read(ctx, &op)
	b.Next(2)
	//return string(op)
	return string(rune(op)), err
}

func (b *PipeReader) read(ctx context.Context, o interface{}) error {
	err := binary.Read(b.reader, binary.BigEndian, o)
	if err == io.EOF {
		b.Done()
	}
	// keep latest bytes (for traces, etc.) - should clear buffer in the end!
	if err == nil && b.needBuffer {
		err = binary.Write(&b.buffer, binary.BigEndian, o) // TODO check performance
		if err != nil {
			log.Error(ctx, err, "Error while writing to buffer")
		}
	}
	return err
}
