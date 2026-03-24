package model

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type (
	Chunk struct {
		Handle         common.Uuid
		StreamType     StreamType
		SequenceId     int
		RotationPeriod uint64
		RotationSize   uint64
		Data           []byte // Alias for Buffer.Bytes() - slice of internal buffer content [valid until the buffer modification]
		buf            *bytes.Buffer
	}
)

func NewChunk(id common.Uuid, streamType StreamType, sequenceId int, rotationPeriod uint64, rotationSize uint64) *Chunk {
	return &Chunk{id, streamType,
		sequenceId, rotationPeriod, rotationSize, nil, nil}
}

func (c *Chunk) InitString(data string) {
	c.Init(&bytes.Buffer{})
	c.Append(data)
}

func (c *Chunk) Init(buf *bytes.Buffer) {
	// TODO sync.Pool
	c.buf = buf
	c.Data = c.buf.Bytes()
}

func (c *Chunk) Append(chunkPart string) {
	c.buf.Write([]byte(chunkPart))
	c.Data = c.buf.Bytes()
}

func (c *Chunk) Bytes() []byte {
	if c.buf != nil { // unserialized
		c.Data = c.buf.Bytes()
	}
	return c.Data
}

func (c *Chunk) Size() int {
	return len(c.Bytes())
}

func (c *Chunk) String() string {
	return fmt.Sprintf("Chunk[%s, %d] (%s, %d bytes)", c.StreamType, c.SequenceId, c.Handle, c.Size())
}

func (c *Chunk) Close() {
	// TODO release buffer to sync.Pool
}

func (c *Chunk) ReplaceByte(ctx context.Context, pos int, v byte) error {
	return c.replace(ctx, pos, 1, v)
}

func (c *Chunk) ReplaceInt(ctx context.Context, pos int, v int) error {
	return c.replace(ctx, pos, 4, uint32(v))
}

func (c *Chunk) ReplaceLong(ctx context.Context, pos int, v uint64) error {
	return c.replace(ctx, pos, 8, v)
}

func (c *Chunk) ReplaceUuid(ctx context.Context, pos int, v common.Uuid) error {
	return c.replace(ctx, pos, 16, v.ToBin())
}

func (c *Chunk) replace(ctx context.Context, pos int, len int, o interface{}) error {
	b, err := c.asBytes(o)
	if err != nil {
		log.Debug(ctx, "error at Pos %d: %+v", pos, err)
	} else {
		// should be extremely careful! change internal slice of our buffer
		for i := 0; i < len; i++ {
			if i+pos < c.Size() {
				c.Data[i+pos] = b[i]
			}
		}
	}
	return err
}

func (c *Chunk) asBytes(o interface{}) ([]byte, error) {
	var b strings.Builder
	err := binary.Write(&b, binary.BigEndian, o)
	return []byte(b.String()), err
}
