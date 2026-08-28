package pipe

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

// StringPipeReader use for sql and xml streams
func StringPipeReader(ctx context.Context, b *PipeReader) <-chan (StringItem) {
	ch := make(chan StringItem)

	go func() {
		defer close(ch)

		i := 0
		strLen := 0
		for !b.EOF() {
			pos := b.Position()
			l, _, st := b.ReadVarString(ctx)
			if l < 0 {
				break // EOF
			}
			log.Trace(ctx, "%d: '%v' [%d/%d]", i, st, l, len(st))
			strLen += len(st)
			if len(st) > 0 {
				ch <- StringItem{i, st, pos}
			}
			i++
		}
		log.Debug(ctx, " * read: EOF. %d lines, %d string chars of %d original bytes", i, strLen, b.Position())
	}()

	return ch
}
