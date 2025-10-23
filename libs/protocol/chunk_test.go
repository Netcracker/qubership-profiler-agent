package model

import (
	"bytes"
	"context"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

func TestChunk_Append(t *testing.T) {
	t.Run("append", func(t *testing.T) {
		c := generateChunk()
		assert.Equal(t, 0, c.Size())
		assert.Equal(t, 0, len(c.Data))

		c.Append("abc")
		assert.Equal(t, 3, c.Size())
		assert.Equal(t, 3, len(c.Data))
		assert.Equal(t, []byte{0x61, 0x62, 0x63}, c.Data)
		assert.Equal(t, "abc", c.buf.String())
		assert.Equal(t, "Chunk[calls, 1] (00:03:00:00:04:00:00:00:00:00:00:00:00:00:00:00, 3 bytes)", c.String())

		c.Append("034ff003")
		assert.Equal(t, 11, c.Size())
		assert.Equal(t, 11, len(c.Data))
		assert.Equal(t, []byte{0x61, 0x62, 0x63, 0x30, 0x33, 0x34, 0x66, 0x66, 0x30, 0x30, 0x33}, c.Data)
		assert.Equal(t, "abc034ff003", c.buf.String())
		assert.Equal(t, "Chunk[calls, 1] (00:03:00:00:04:00:00:00:00:00:00:00:00:00:00:00, 11 bytes)", c.String())
	})

}

func TestChunk_Replace(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	gen := func() *Chunk {
		c := generateChunk()
		c.Append("abc034ff003")
		assert.Equal(t, 11, c.Size())
		assert.Equal(t, []byte{0x61, 0x62, 0x63, 0x30, 0x33, 0x34, 0x66, 0x66, 0x30, 0x30, 0x33}, c.Data)
		return c
	}

	t.Run("replace", func(t *testing.T) {
		t.Run("bytes", func(t *testing.T) {
			c := gen()

			err := c.ReplaceByte(ctx, 9, 125)
			assert.Nil(t, err)
			assert.Equal(t, 11, c.Size())
			assert.Equal(t, 11, len(c.Data))
			assert.Equal(t, []byte{0x61, 0x62, 0x63, 0x30, 0x33, 0x34, 0x66, 0x66, 0x30, 0x7d, 0x33}, c.Data)
			assert.Equal(t, "abc034ff0}3", c.buf.String())
			assert.Equal(t, "Chunk[calls, 1] (00:03:00:00:04:00:00:00:00:00:00:00:00:00:00:00, 11 bytes)", c.String())
		})

		t.Run("int", func(t *testing.T) {
			c := gen()

			err := c.ReplaceInt(ctx, 9, 235034)
			assert.Nil(t, err)
			assert.Equal(t, 11, c.Size())
			assert.Equal(t, 11, len(c.Data))
			assert.Equal(t, []byte{0x61, 0x62, 0x63, 0x30, 0x33, 0x34, 0x66, 0x66, 0x30, 0x0, 0x3}, c.Data)
			assert.Equal(t, "abc034ff0\x00\x03", c.buf.String())

			err = c.ReplaceInt(ctx, 5, 235035)
			assert.Nil(t, err)
			assert.Equal(t, 11, c.Size())
			assert.Equal(t, 11, len(c.Data))
			assert.Equal(t, []byte{0x61, 0x62, 0x63, 0x30, 0x33, 0x0, 0x3, 0x96, 0x1b, 0x0, 0x3}, c.Data)
			assert.Equal(t, "abc03\x00\x03\x96\x1b\x00\x03", c.buf.String())

			err = c.ReplaceInt(ctx, 5, 235034)
			assert.Nil(t, err)
			assert.Equal(t, 11, c.Size())
			assert.Equal(t, 11, len(c.Data))
			assert.Equal(t, []byte{0x61, 0x62, 0x63, 0x30, 0x33, 0x0, 0x3, 0x96, 0x1a, 0x0, 0x3}, c.Data)
			assert.Equal(t, "abc03\x00\x03\x96\x1a\x00\x03", c.buf.String())
		})

		t.Run("long", func(t *testing.T) {
			c := gen()

			err := c.ReplaceLong(ctx, 1, 123)
			assert.Nil(t, err)
			assert.Equal(t, 11, c.Size())
			assert.Equal(t, 11, len(c.Data))
			assert.Equal(t, []byte{0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7b, 0x30, 0x33}, c.Data)
			assert.Equal(t, "a\x00\x00\x00\x00\x00\x00\x00{03", c.buf.String())

			err = c.ReplaceLong(ctx, 1, 126)
			assert.Nil(t, err)
			assert.Equal(t, 11, c.Size())
			assert.Equal(t, 11, len(c.Data))
			assert.Equal(t, []byte{0x61, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7e, 0x30, 0x33}, c.Data)
			assert.Equal(t, "a\x00\x00\x00\x00\x00\x00\x00~03", c.buf.String())

			err = c.ReplaceLong(ctx, 1, 565358957869934592)
			assert.Nil(t, err)
			assert.Equal(t, 11, c.Size())
			assert.Equal(t, 11, len(c.Data))
			assert.Equal(t, []byte{0x61, 0x7, 0xd8, 0x8e, 0xfa, 0xe, 0x6c, 0x28, 0x0, 0x30, 0x33}, c.Data)
			assert.Equal(t, "a\a؎\xfa\x0el(\x0003", c.buf.String())

			err = c.ReplaceLong(ctx, 9, 123)
			assert.Nil(t, err)
			assert.Equal(t, 11, c.Size())
			assert.Equal(t, 11, len(c.Data))
			assert.Equal(t, []byte{0x61, 0x7, 0xd8, 0x8e, 0xfa, 0xe, 0x6c, 0x28, 0x0, 0x0, 0x0}, c.Data)
			assert.Equal(t, "a\a؎\xfa\x0el(\x00\x00\x00", c.buf.String())

		})
	})
}

func TestChunk_ReplaceUuid(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	gen := func() *Chunk {
		c := generateChunk()
		c.Append("abc034ff003abc034ff003")
		assert.Equal(t, 22, c.Size())
		assert.Equal(t, []byte{
			0x61, 0x62, 0x63, 0x30, 0x33, 0x34, 0x66, 0x66, 0x30, 0x30, 0x33,
			0x61, 0x62, 0x63, 0x30, 0x33, 0x34, 0x66, 0x66, 0x30, 0x30, 0x33}, c.Data)
		return c
	}

	t.Run("uuid", func(t *testing.T) {
		c := gen()

		uuid := common.ToUuid([16]byte{1: 3, 4: 4})
		err := c.ReplaceUuid(ctx, 1, uuid)
		assert.Nil(t, err)
		assert.Equal(t, 22, c.Size())
		assert.Equal(t, 22, len(c.Data))
		assert.Equal(t, []byte{
			0x61,
			0x0, 0x3, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0,
			0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
			0x66, 0x66, 0x30, 0x30, 0x33}, c.Data)
		assert.Equal(t, "a\x00\x03\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00ff003", c.buf.String())
		assert.Equal(t, "Chunk[calls, 1] (00:03:00:00:04:00:00:00:00:00:00:00:00:00:00:00, 22 bytes)", c.String())

	})
}

func generateChunk() *Chunk {
	c := NewChunk(
		common.ToUuid([16]byte{1: 3, 4: 4}), // TODO: common.RandomUuid() ?
		StreamCalls, 1, 123, 1_000_000)
	c.Init(&bytes.Buffer{})
	return c
}
