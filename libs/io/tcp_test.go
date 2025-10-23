package io

import (
	"bytes"
	"context"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

func TestPrepareTcpReader(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	t.Run("byte", func(t *testing.T) {
		list := []byte{34, 5, 10, 254, 255, 100}
		tw, tr := prepareChannel(t)

		for i, v := range list {
			err := tw.WriteFixedByte(ctx, v)
			assert.Nil(t, err)
			err = tw.Flush()
			assert.Nil(t, err)

			assert.Equal(t, uint64(i), tr.pos)
			got, err := tr.ReadFixedByte(ctx)
			assert.Nil(t, err)
			assert.Equal(t, uint64(i+1), tr.pos)
			assert.Equal(t, v, got)
		}
	})
	t.Run("int", func(t *testing.T) {
		list := []int{34, 5, 10, 254, 255, 100, 54, 450, 34054, 5445454, 123213123}
		// list := []int{1, 0, -1, -2, -3} // only works for positive numbers
		tw, tr := prepareChannel(t)

		for i, v := range list {
			err := tw.WriteFixedInt(ctx, v)
			assert.Nil(t, err)
			err = tw.Flush()
			assert.Nil(t, err)

			assert.Equal(t, uint64(4*i), tr.pos)
			got, err := tr.ReadFixedInt(ctx)
			assert.Nil(t, err)
			assert.Equal(t, uint64(4*i+4), tr.pos)
			assert.Equal(t, v, got)
		}
	})
	t.Run("long", func(t *testing.T) { // actually, uint64
		list := []uint64{34, 5, 10, 254, 255, 100, 54, 450, 34054, 5445454, 123213123}
		tw, tr := prepareChannel(t)

		for i, v := range list {
			err := tw.WriteFixedLong(ctx, v)
			assert.Nil(t, err)
			err = tw.Flush()
			assert.Nil(t, err)

			assert.Equal(t, uint64(8*i), tr.pos)
			got, err := tr.ReadFixedLong(ctx)
			assert.Nil(t, err)
			assert.Equal(t, uint64(8*i+8), tr.pos)
			assert.Equal(t, v, got)
		}
	})
	t.Run("uuid", func(t *testing.T) {
		list := []common.Uuid{
			common.ToUuid([16]byte{1}),
			common.ToUuid([16]byte{1, 4, 0, 65, 23, 45}),
		}
		tw, tr := prepareChannel(t)

		for i, v := range list {
			err := tw.WriteUuid(ctx, v)
			assert.Nil(t, err)
			err = tw.Flush()
			assert.Nil(t, err)

			assert.Equal(t, uint64(16*i), tr.pos)
			got, err := tr.ReadUuid(ctx)
			assert.Nil(t, err)
			assert.Equal(t, uint64(16*i+16), tr.pos)
			assert.Equal(t, v, got)
		}
	})
	t.Run("string", func(t *testing.T) {
		list := []string{
			"word1",
			"worddsfdsfsdfsdf",
			"",
			"word0",
			"000000",
		}
		tw, tr := prepareChannel(t)

		var p uint64
		for _, v := range list {
			err := tw.WriteFixedString(ctx, v)
			assert.Nil(t, err)
			err = tw.Flush()
			assert.Nil(t, err)

			assert.Equal(t, p, tr.pos)
			got, err := tr.ReadFixedString(ctx)
			assert.Nil(t, err)
			p += uint64(4 + len(v))
			assert.Equal(t, p, tr.pos)
			assert.Equal(t, v, got)
		}
	})

	t.Run("buf", func(t *testing.T) {
		list := []string{
			"word1",
			"worddsfdsfsdfsdf",
			"",
			"word0",
			"000000",
		}
		tw, tr := prepareChannel(t)

		var p uint64
		for _, v := range list {
			err := tw.WriteFixedBuf(ctx, []byte(v))
			assert.Nil(t, err)
			err = tw.Flush()
			assert.Nil(t, err)

			assert.Equal(t, p, tr.pos)
			got, err := tr.ReadFixedString(ctx)
			assert.Nil(t, err)
			p += uint64(4 + len(v))
			assert.Equal(t, p, tr.pos)
			assert.Equal(t, v, got)
		}
	})

	t.Run("char", func(t *testing.T) {
		tw, tr := prepareChannel(t)

		err := tw.WriteFixedBuf(ctx, []byte{0x4a, 0x00})
		assert.Nil(t, err)
		err = tw.Flush()
		assert.Nil(t, err)

		assert.Equal(t, uint64(0), tr.pos)
		ch := tr.readChar(ctx)
		assert.Equal(t, uint64(2), tr.pos)
		assert.Equal(t, "\x00", ch) // TODO fix chars
	})
}

func prepareChannel(t *testing.T) (*TcpWriter, *TcpReader) {
	buf := &bytes.Buffer{}
	tw := PrepareTcpWriter(buf)
	assert.NotNil(t, tw)
	tr := PrepareTcpReader(buf)
	assert.NotNil(t, tr)
	return tw, tr
}

//func TestTcpReader_ReadFixedString(t *testing.T) {
//	t.Run("", func(t *testing.T) {
//		tr := &TcpReader{
//			reader: tt.fields.reader,
//			err:    tt.fields.err,
//			pos:    tt.fields.pos,
//			debug:  tt.fields.debug,
//		}
//		got, err := tr.ReadFixedString(tt.args.ctx)
//		if (err != nil) != tt.wantErr {
//			t.Errorf("ReadFixedString() error = %v, wantErr %v", err, tt.wantErr)
//			return
//		}
//		if got != tt.want {
//			t.Errorf("ReadFixedString() got = %v, want %v", got, tt.want)
//		}
//	})
//}
//
//func TestTcpWriter_WriteFixedBuf(t *testing.T) {
//	t.Run("", func(t *testing.T) {
//		tw := &TcpWriter{
//			writer: tt.fields.writer,
//			sent:   tt.fields.sent,
//			debug:  tt.fields.debug,
//		}
//		if err := tw.WriteFixedBuf(tt.args.ctx, tt.args.v); (err != nil) != tt.wantErr {
//			t.Errorf("WriteFixedBuf() error = %v, wantErr %v", err, tt.wantErr)
//		}
//	})
//}
//func TestTcpWriter_WriteFixedString(t *testing.T) {
//	t.Run("", func(t *testing.T) {
//		tw := &TcpWriter{
//			writer: tt.fields.writer,
//			sent:   tt.fields.sent,
//			debug:  tt.fields.debug,
//		}
//		if err := tw.WriteFixedString(tt.args.ctx, tt.args.v); (err != nil) != tt.wantErr {
//			t.Errorf("WriteFixedString() error = %v, wantErr %v", err, tt.wantErr)
//		}
//	})
//}
//
