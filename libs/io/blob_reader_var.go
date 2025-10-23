package io

import (
	"context"
	"strings"
)

// complex parsing

func (b *BlobReader) ReadVarInt(ctx context.Context) int {
	read := func() int {
		var x uint8
		b.read(ctx, &x)
		b.Next()
		return int(x)
	}
	var res int

	x := read()
	if x == -1 {
		return -1
	} else if (x & 0x80) == 0 {
		return x
	}
	res = x & ^0x80

	x = read()
	res |= x << 7
	if (res & (0x80 << 7)) == 0 {
		return res
	}
	res &= ^(0x80 << 7)

	x = read()
	res = res | x<<14
	if (res & (0x80 << 14)) == 0 {
		return res
	}
	res &= ^(0x80 << 14)

	x = read()
	res |= x << 21
	if (res & (0x80 << 21)) == 0 {
		return res
	}
	res &= ^(0x80 << 21)

	x = read()
	res |= x << 28
	return res
}

func (b *BlobReader) ReadVarLong(ctx context.Context) int64 {
	read := func() int64 {
		var x byte
		b.read(ctx, &x)
		b.Next()
		return int64(x)
	}
	var res int64

	x := read()
	if x == -1 {
		return -1
	} else if (x & 0x80) == 0 {
		return x
	}
	res = x & ^0x80 // 0..6

	x = read()
	res |= x << 7
	if (res & (0x80 << 7)) == 0 {
		return res
	}
	res &= ^(0x80 << 7) // 7..13

	x = read()
	res = res | x<<14
	if (res & (0x80 << 14)) == 0 {
		return res
	}
	res &= ^(0x80 << 14) // 14..20

	x = read()
	res |= x << 21
	if (res & (0x80 << 21)) == 0 {
		return res
	}
	res &= ^(0x80 << 21) // 21..28

	x = read()
	if (x & 0x80) == 0 {
		return (x << 28) | res
	}
	resLong := ((x & 0x7f) << 28) | res
	return (int64(b.ReadVarInt(ctx)) << 35) | resLong
}

func (b *BlobReader) ReadVarIntZigZag(ctx context.Context) int {
	res := b.ReadVarInt(ctx)
	res = (res >> 1) ^ (-(res & 1))
	return res
}

func (b *BlobReader) ReadVarString(ctx context.Context) (int, int, string) {
	curr := b.pos
	maxLength := 10 * 1024 * 1024
	//b.PrintDebug()
	length := b.ReadVarInt(ctx)
	//b.PrintDebug()
	if length > maxLength {
		//throw new IOException("Expecting string of max length " + maxLength + ", got " + length
		//+ " chars; position = " + position);
		return 0, 0, ""
	}
	var sb strings.Builder
	for i := 0; i < length; i++ {
		char, _ := b.readChar(ctx)
		sb.WriteString(char)
	}
	return length, b.pos - curr, sb.String()
}
