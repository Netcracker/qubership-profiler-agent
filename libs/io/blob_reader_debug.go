package io

import (
	"context"
	"fmt"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

// debugging

func (b *BlobReader) get(n uint32) []byte {
	if b.EOF() {
		return []byte("")
	}
	l := b.pos + int(n)
	if l > b.Len {
		l = b.Len
	}
	return b.Data[b.pos:l]
}

func (b *BlobReader) print(ctx context.Context, val interface{}, n uint32) {
	if !log.IsExtraTraceEnabled(ctx) {
		return
	}
	var bb strings.Builder
	fmt.Fprintf(&bb, "Pos [%d/%X/%d]: ", b.Pos(), b.Pos(), b.Len)
	for _, c := range b.get(n) {
		fmt.Fprintf(&bb, "0x%02X ", c)
	}
	if val != nil {
		fmt.Fprintf(&bb, " (val=%v)", val)
	}
	log.ExtraTrace(ctx, bb.String())
}

func (b *BlobReader) short(ctx context.Context, val string, n uint32, len uint32) {
	if !log.IsExtraTraceEnabled(ctx) {
		return
	}
	var bb strings.Builder
	fmt.Fprintf(&bb, "Pos [%d/%X/%d]s: ", b.Pos(), b.Pos(), b.Len)
	for _, c := range b.get(n) {
		fmt.Fprintf(&bb, "0x%02X ", c)
	}
	x := uint32(b.pos) + len
	fmt.Fprintf(&bb, " ... ")

	if len > 50000 {
		panic(val)
	}
	for _, c := range b.Data[x-4 : x] {
		fmt.Fprintf(&bb, "0x%02X ", c)
	}
	fmt.Fprintf(&bb, " (val=%v...%v)", val[0:n], val[:n-4])
	log.ExtraTrace(ctx, bb.String())
}

func (b *BlobReader) PrintDebug(ctx context.Context) {
	if !log.IsTraceEnabled(ctx) {
		return
	}
	b.print(ctx, nil, 9)
}
