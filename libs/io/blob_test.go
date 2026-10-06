package io

import (
	"context"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

func TestBytes(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	t.Run("bytes", func(t *testing.T) {
		tr := reader(t, []byte{})
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{1})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		r, err := tr.ReadFixedByte(ctx)
		assert.Nil(t, err)
		assert.Equal(t, byte(1), r)
		assert.Equal(t, uint64(1), tr.Pos())
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{1, 102, 34})
		assert.Nil(t, err)
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())

		r, err = tr.ReadFixedByte(ctx)
		assert.Nil(t, err)
		assert.Equal(t, byte(1), r)
		assert.Equal(t, uint64(1), tr.Pos())
		assert.False(t, tr.EOF())

		r, err = tr.ReadFixedByte(ctx)
		assert.Nil(t, err)
		assert.Equal(t, byte(102), r)
		assert.Equal(t, uint64(2), tr.Pos())
		assert.False(t, tr.EOF())

		r, err = tr.ReadFixedByte(ctx)
		assert.Nil(t, err)
		assert.Equal(t, byte(34), r)
		assert.Equal(t, uint64(3), tr.Pos())
		assert.True(t, tr.EOF())

	})
}

func TestDoneEOF(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	t.Run("eof", func(t *testing.T) {
		tr := reader(t, []byte{})
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{1, 102, 34})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())

		r, err := tr.ReadFixedByte(ctx)
		assert.Nil(t, err)
		assert.Equal(t, byte(1), r)
		assert.Equal(t, uint64(1), tr.Pos())
		assert.False(t, tr.EOF())

		tr.Done()
		assert.True(t, tr.EOF())

	})
}

func TestInt(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	t.Run("int", func(t *testing.T) {
		tr := reader(t, []byte{0, 0, 0, 0})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		r, err := tr.ReadFixedInt(ctx)
		assert.Nil(t, err)
		assert.Equal(t, int(0), r)
		assert.Equal(t, uint64(4), tr.Pos())
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{4, 0, 0, 1})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		r, err = tr.ReadFixedInt(ctx)
		assert.Nil(t, err)
		assert.Equal(t, int(67108865), r)
		assert.Equal(t, uint64(4), tr.Pos())
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{0, 0, 0, 1, 4, 0, 0, 2})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		r, err = tr.ReadFixedInt(ctx)
		assert.Nil(t, err)
		assert.Equal(t, int(1), r)
		assert.Equal(t, uint64(4), tr.Pos())
		assert.False(t, tr.EOF())
		r, err = tr.ReadFixedInt(ctx)
		assert.Nil(t, err)
		assert.Equal(t, int(67108866), r)
		assert.Equal(t, uint64(8), tr.Pos())
		assert.True(t, tr.EOF())
	})
}

func TestLong(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	t.Run("long", func(t *testing.T) {
		tr := reader(t, []byte{0, 0, 0, 0, 0, 0, 0, 0})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		r, err := tr.ReadFixedLong(ctx)
		assert.Nil(t, err)
		assert.Equal(t, uint64(0), r)
		assert.Equal(t, uint64(8), tr.Pos())
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{0, 0, 0, 0, 4, 0, 0, 1})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		r, err = tr.ReadFixedLong(ctx)
		assert.Nil(t, err)
		assert.Equal(t, uint64(67108865), r)
		assert.Equal(t, uint64(8), tr.Pos())
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 4, 0, 0, 2})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		r, err = tr.ReadFixedLong(ctx)
		assert.Nil(t, err)
		assert.Equal(t, uint64(1), r)
		assert.Equal(t, uint64(8), tr.Pos())
		assert.False(t, tr.EOF())
		r, err = tr.ReadFixedLong(ctx)
		assert.Nil(t, err)
		assert.Equal(t, uint64(67108866), r)
		assert.Equal(t, uint64(16), tr.Pos())
		assert.True(t, tr.EOF())
	})
}

func TestUuid(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	t.Run("uuid", func(t *testing.T) {
		var arr []byte
		uuid := [16]byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 4, 0, 0, 2}
		for _, i := range uuid {
			arr = append(arr, i)
		}

		tr := reader(t, arr)
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		r, err := tr.ReadUuid(ctx)
		assert.Nil(t, err)
		assert.Equal(t, common.ToUuid(uuid), r)
		assert.Equal(t, uint64(16), tr.Pos())
		assert.True(t, tr.EOF())
	})
}

func TestString(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	t.Run("string", func(t *testing.T) {
		tr := reader(t, append([]byte{0, 0, 0, 2}, []byte("3m")...))
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		r, err := tr.ReadFixedString(ctx)
		assert.Nil(t, err)
		assert.Equal(t, "3m", r)
		assert.Equal(t, uint64(6), tr.Pos())
		assert.True(t, tr.EOF())
	})
}

func TestVarInt(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	t.Run("var int", func(t *testing.T) {
		tr := reader(t, []byte{34, 0, 0, 0})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int(34), tr.ReadVarInt(ctx))
		assert.Equal(t, uint64(1), tr.Pos())
		assert.False(t, tr.EOF())

		tr = reader(t, []byte{34})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int(34), tr.ReadVarInt(ctx))
		assert.Equal(t, uint64(1), tr.Pos())
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{130, 36, 54})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int(4610), tr.ReadVarInt(ctx)) // TODO check
		assert.Equal(t, uint64(2), tr.Pos())
		assert.False(t, tr.EOF())

		// TODO cases for big values

		tr = reader(t, []byte{0x90, 0xA4, 54})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int(889360), tr.ReadVarInt(ctx))
		assert.Equal(t, uint64(3), tr.Pos())
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{0x90, 0xA4, 0xE3, 40})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int(85512720), tr.ReadVarInt(ctx))
		assert.Equal(t, uint64(4), tr.Pos())
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{0x90, 0xA4, 0xE3, 0x94})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int(43569680), tr.ReadVarInt(ctx))
		assert.Equal(t, uint64(5), tr.Pos()) // TODO double check
		assert.True(t, tr.EOF())

	})

	t.Run("var int", func(t *testing.T) {
		tr := reader(t, []byte{34, 0, 0, 0})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int(17), tr.ReadVarIntZigZag(ctx)) // TODO double check
		assert.Equal(t, uint64(1), tr.Pos())
		assert.False(t, tr.EOF())
	})
}

func TestVarLong(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	t.Run("var long", func(t *testing.T) {
		tr := reader(t, []byte{})
		assert.True(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int64(0), tr.ReadVarLong(ctx))
		assert.Equal(t, uint64(1), tr.Pos()) // TODO is it correct for empty data?
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{34, 0, 0, 0})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int64(34), tr.ReadVarLong(ctx))
		assert.Equal(t, uint64(1), tr.Pos())
		assert.False(t, tr.EOF())

		tr = reader(t, []byte{34})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int64(34), tr.ReadVarLong(ctx))
		assert.Equal(t, uint64(1), tr.Pos())
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{130, 36, 54})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int64(4610), tr.ReadVarLong(ctx)) // TODO check
		assert.Equal(t, uint64(2), tr.Pos())
		assert.False(t, tr.EOF())

		// TODO cases for big values

		tr = reader(t, []byte{0x93, 0xA4, 54})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int64(889363), tr.ReadVarLong(ctx))
		assert.Equal(t, uint64(3), tr.Pos())
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{0x93, 0xA4, 0xE4})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int64(1643027), tr.ReadVarLong(ctx))
		assert.Equal(t, uint64(4), tr.Pos())
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{0x93, 0xA4, 0xE4, 0x9b})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int64(58266131), tr.ReadVarLong(ctx))
		assert.Equal(t, uint64(5), tr.Pos()) // TODO double check
		assert.True(t, tr.EOF())

		tr = reader(t, []byte{0x93, 0xA4, 0xE4, 0x9b, 0xAE})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		assert.Equal(t, int64(12406297107), tr.ReadVarLong(ctx))
		assert.Equal(t, uint64(6), tr.Pos()) // TODO double check
		assert.True(t, tr.EOF())
	})
}

func TestVarString(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	t.Run("var string", func(t *testing.T) {
		tr := reader(t, []byte{4, 0, 33, 0, 32, 0, 56, 0, 58})
		assert.False(t, tr.EOF())
		assert.Equal(t, uint64(0), tr.Pos())
		n, _, s := tr.ReadVarString(ctx)
		assert.Equal(t, 4, n)
		assert.Equal(t, "! 8:", s)
		assert.Equal(t, uint64(9), tr.Pos())
		assert.True(t, tr.EOF())
	})
}

func TestDebugFunctions(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.EXTRA)
	t.Run("short fixed string", func(t *testing.T) {
		s := log.CaptureAsString(func() {
			tr := reader(t, append([]byte{0, 0, 0, 2}, []byte("3m")...))
			r, err := tr.ReadFixedString(ctx)
			assert.Nil(t, err)
			assert.Equal(t, "3m", r)
			assert.Equal(t, uint64(6), tr.Pos())
			assert.True(t, tr.EOF())
		})
		assert.Equal(t, "[2006-01-02T01:02:03.004] [extra] [request_id=-] [tenant_id=--] [thread=-] [class=io/blob_test.go:12] Pos [4/4/6]: 0x33 0x6D  (val=3m)\n", s, "should be debug logs")
	})
	t.Run("long fixed string", func(t *testing.T) {
		s := log.CaptureAsString(func() {
			tr := reader(t, append([]byte{0, 0, 0, 33}, []byte("0123456789a0123456789b0123456789c")...))
			r, err := tr.ReadFixedString(ctx)
			assert.Nil(t, err)
			assert.Equal(t, "0123456789a0123456789b0123456789c", r)
			assert.Equal(t, uint64(37), tr.Pos())
			assert.True(t, tr.EOF())
		})
		assert.Equal(t, "[2006-01-02T01:02:03.004] [extra] [request_id=-] [tenant_id=--] [thread=-] [class=io/blob_test.go:12] Pos [4/4/37]s: 0x30 0x31 0x32 0x33 0x34 0x35 0x36 0x37 0x38 0x39  ... 0x37 0x38 0x39 0x63  (val=0123456789...012345)\n", s, "should be debug logs")
	})
	t.Run("print debug", func(t *testing.T) {
		s := log.CaptureAsString(func() {
			tr := reader(t, append([]byte{0, 0, 0, 33}, []byte("0123456789a0123456789b0123456789c")...))
			assert.False(t, tr.EOF())
			tr.PrintDebug(ctx)
			assert.False(t, tr.EOF())
		})
		assert.Equal(t, "[2006-01-02T01:02:03.004] [extra] [request_id=-] [tenant_id=--] [thread=-] [class=io/blob_test.go:12] Pos [0/0/37]: 0x00 0x00 0x00 0x21 0x30 0x31 0x32 0x33 0x34 \n", s, "should be debug logs")
	})
	t.Run("don't print without extra log", func(t *testing.T) {
		s := log.CaptureAsString(func() {
			tr := reader(t, append([]byte{0, 0, 0, 33}, []byte("0123456789a0123456789b0123456789c")...))
			ctx = log.SetLevel(ctx, log.TRACE)
			assert.False(t, tr.EOF())
			tr.PrintDebug(ctx)
			assert.False(t, tr.EOF())
		})
		assert.Equal(t, "", s, "should not be debug logs in just trace")
	})
}

func reader(t *testing.T, arr []byte) *BlobReader {
	tr, err := NewBlobReader(arr)
	assert.Nil(t, err)
	assert.NotNil(t, tr)
	return tr
}
