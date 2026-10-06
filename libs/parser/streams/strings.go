package streams

import (
	"context"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

func ReadStringStream(ctx context.Context, stream model.StreamType, c *model.Chunk) string {
	b := AsBlob(c)
	//debug = true
	log.Debug(ctx, " * reading '%s': %s", stream, c.String())

	b.PrintDebug(ctx)
	i := 0
	strLen := 0
	var s strings.Builder
	for !b.EOF() {
		i++
		l, _, st := b.ReadVarString(ctx)
		log.Trace(ctx, "%d: '%v' [%d/%d]", i, st, l, len(st))
		s.WriteString(st + "\n")
		strLen += len(st)
	}

	log.Debug(ctx, " * read '%s': EOF. %d lines, %d string bytes of %d model bytes (%d overhead) ",
		stream, i, 2*strLen, c.Size(), c.Size()-2*strLen)
	return s.String()
}
