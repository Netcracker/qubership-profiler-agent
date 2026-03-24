package io

import (
	"bytes"
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestReaderInterface(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	var err error

	t.Run("EOF", func(t *testing.T) {
		t.Run("blob reader", func(t *testing.T) {
			var a Reader
			a, err = NewBlobReader([]byte{})
			assert.Nil(t, err)
			assert.True(t, a.EOF())
			_, err = a.ReadFixedByte(ctx)
			assert.ErrorContains(t, err, "EOF")
			assert.Equal(t, io.EOF, err)
			assert.True(t, a.EOF())
		})
		t.Run("tcp reader", func(t *testing.T) {
			var a Reader
			buf := &bytes.Buffer{}
			a = PrepareTcpReader(buf)
			assert.False(t, a.EOF())
			_, err = a.ReadFixedByte(ctx)
			assert.ErrorContains(t, err, "EOF")
			assert.Equal(t, io.EOF, err)
			assert.True(t, a.EOF())
		})
	})
}

func TestWriterInterface(t *testing.T) {
	var err error

	t.Run("Flush", func(t *testing.T) {
		t.Run("tcp writer", func(t *testing.T) {
			var a Writer
			buf := &bytes.Buffer{}
			a = PrepareTcpWriter(buf)
			err = a.Flush()
			assert.Nil(t, err)
		})
	})
}
