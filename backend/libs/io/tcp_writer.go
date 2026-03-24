package io

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type (
	TcpWriter struct {
		//writer io.Writer
		writer *bufio.Writer
		sent   uint64
		debug  bool
	}
)

func PrepareTcpWriter(writer io.Writer) *TcpWriter {
	bw := bufio.NewWriterSize(writer, 8*1024)
	return &TcpWriter{bw, 0, false}
}

func (tw *TcpWriter) Flush() error {
	return tw.writer.Flush()
}

// simple parsing

func (tw *TcpWriter) WriteFixedByte(ctx context.Context, v byte) error {
	return tw.write(ctx, 1, v)
}

func (tw *TcpWriter) WriteFixedInt(ctx context.Context, v int) error {
	return tw.write(ctx, 4, uint32(v))
}

func (tw *TcpWriter) WriteFixedLong(ctx context.Context, v uint64) error {
	return tw.write(ctx, 8, v)
}

func (tw *TcpWriter) WriteUuid(ctx context.Context, v common.Uuid) error {
	return tw.write(ctx, 16, v.ToBin())
}

func (tw *TcpWriter) WriteFixedString(ctx context.Context, v string) error {
	err := tw.write(ctx, 4, uint32(len(v)))
	if err == nil {
		err = tw.write(ctx, 2*len(v), []byte(v))
	}
	return err
}

func (tw *TcpWriter) WriteFixedBuf(ctx context.Context, v []byte) error {
	err := tw.write(ctx, 4, uint32(len(v)))
	if err == nil {
		err = tw.write(ctx, 2*len(v), v)
	}
	return err
}

func (tw *TcpWriter) write(ctx context.Context, c int, o interface{}) error {
	err := binary.Write(tw.writer, binary.BigEndian, o)
	if err != nil {
		log.Debug(ctx, "error at Pos %d: %+v", tw.sent, err)
	} else {
		log.Trace(ctx, "-> #%5d, send %d bytes: %s", tw.sent, binary.Size(o), common.AsHex(asBytes(o), 30))
		tw.sent += uint64(c)
	}
	return err
}
