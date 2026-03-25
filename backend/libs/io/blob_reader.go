package io

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type (
	BlobReader struct {
		Data   []byte
		Len    int // =int64 on 64bit OS
		Reader *bytes.Reader
		pos    int
		isDone bool
	}
)

func NewBlobReader(data []byte) (*BlobReader, error) {
	reader := bytes.NewReader(data)
	return &BlobReader{data, len(data), reader, 0, false}, nil
}

func OpenFileAsBlob(filename string) (*BlobReader, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		//panic(err)
		return nil, err
	}
	reader := bytes.NewReader(data)
	return &BlobReader{data, len(data), reader, 0, false}, nil
}

func (b *BlobReader) EOF() bool {
	if b.isDone {
		return true
	}
	f := b.pos >= b.Len
	if f {
		b.isDone = true
	}
	return f
}

func (b *BlobReader) Done() {
	b.isDone = true
}

func (b *BlobReader) Pos() uint64 {
	return uint64(b.pos)
}

func (b *BlobReader) Next(i ...uint32) bool {
	if len(i) == 1 {
		b.pos += int(i[0])
	} else {
		b.pos++
	}
	return b.EOF()
}

// simple parsing

func (b *BlobReader) ReadFixedByte(ctx context.Context) (byte, error) {
	var op byte
	err := b.read(ctx, &op)
	b.Next()
	return op, err
}

func (b *BlobReader) ReadFixedInt(ctx context.Context) (int, error) {
	var op uint32
	err := b.read(ctx, &op)
	b.Next(4)
	return int(op), err
}
func (b *BlobReader) ReadFixedLong(ctx context.Context) (uint64, error) {
	var op uint64
	err := b.read(ctx, &op)
	b.Next(8)
	return op, err
}
func (b *BlobReader) ReadUuid(ctx context.Context) (common.Uuid, error) {
	data := make([]byte, 16)
	err := b.read(ctx, data)
	o := [16]byte{}
	for i := 0; i < 16; i++ {
		o[i] = data[i]
	}
	b.print(ctx, common.ToHex(o), 16)
	b.Next(16)
	return common.ToUuid(o), err
}

func (b *BlobReader) ReadFixedString(ctx context.Context) (string, error) {
	length, err := b.readLen(ctx)
	// TODO safety check (warning and limit for length > 2mb ?)
	//  * reason: must be err, because we have limiter (4kb for large objects) on the profiler agent side
	data := make([]byte, length)
	err = b.read(ctx, data)
	if length <= 30 {
		b.print(ctx, string(data), length)
	} else {
		b.short(ctx, string(data), 10, length)
	}
	b.Next(length)
	return string(data), err
}

func (b *BlobReader) readLen(ctx context.Context) (uint32, error) {
	var op uint32
	err := b.read(ctx, &op)
	b.Next(4)
	return op, err
}

func (b *BlobReader) readChar(ctx context.Context) (string, error) {
	var op int16
	err := b.read(ctx, &op)
	b.Next(2)
	//return string(op)
	return string(rune(op)), err
}

func (b *BlobReader) read(ctx context.Context, o interface{}) error {
	_, err := b.Reader.Seek(int64(b.pos), io.SeekStart)
	if err == nil {
		err = binary.Read(b.Reader, binary.BigEndian, o)
	} else {
		log.Debug(ctx, "error at Pos %d: %+v", b.Pos(), err)
	}
	return err
}
