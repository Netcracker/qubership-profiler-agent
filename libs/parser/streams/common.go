package streams

import (
	"fmt"

	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
)

type Histogram struct {
	histogram [5]int
}

func (h *Histogram) Mark(length int) {
	n := nBytes(length)
	if n < 0 || n >= 4 {
		panic(fmt.Errorf("invalid length: %d", length))
	}
	h.histogram[n]++
}

func (h *Histogram) String() string {
	return fmt.Sprintf("h[0b: %d, 1b: %d, 2b: %d, 3b: %d, 4b: %d]",
		h.histogram[0], h.histogram[1], h.histogram[2], h.histogram[3], h.histogram[4])
}

func AsBlob(c *model.Chunk) *io.BlobReader {
	reader, _ := io.NewBlobReader(c.Bytes())
	return reader
}

func nBytes(l int) int { // number of bytes to save int
	c := 0
	for l > 0 {
		l = l >> 8
		c++
	}
	return c
}
