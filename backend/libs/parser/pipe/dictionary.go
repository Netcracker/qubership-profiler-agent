package pipe

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

func DictionaryPipeReader(ctx context.Context, b *PipeReader, limitPhrases int) <-chan DictionaryItem {
	ch := make(chan DictionaryItem)

	go func() {
		defer close(ch)

		var err error
		lines := 0
		phrases := 0

		lengthOfPhrase := -1
		for !b.EOF() {
			if lengthOfPhrase <= 0 {
				phrases++
				if phrases > limitPhrases {
					break
				}

				lengthOfPhrase, err = b.ReadFixedInt(ctx)
				if err != nil || b.EOF() || lengthOfPhrase < 0 {
					break
				}
			}
			pos := b.Position()
			length, bytes, st := b.ReadVarString(ctx)
			if b.EOF() || length < 0 {
				break
			}
			lengthOfPhrase -= bytes
			if len(st) > 0 {
				ch <- DictionaryItem{Id: lines, Value: st, pos: pos}
				lines++
			}
		}
		log.Debug(ctx, " * read: EOF. %d lines, %d phrases of %d original bytes", lines, phrases, b.Position())
	}()

	return ch
}

func DictionaryV2PipeReader(ctx context.Context, b *PipeReader) <-chan DictionaryItem {
	ch := make(chan DictionaryItem)

	go func() {
		defer close(ch)

		i := 0
		strLen := 0
		for !b.EOF() {
			id, err := b.ReadVarInt(ctx)
			if err != nil {
				break // EOF
			}
			pos := b.Position()
			l, _, st := b.ReadVarString(ctx)
			if l < 0 {
				break // EOF
			}
			strLen += len(st)
			if len(st) > 0 {
				ch <- DictionaryItem{id, st, pos}
			}
			i++
		}
		log.Debug(ctx, " * read: EOF. %d lines, %d string chars of %d original bytes", i, strLen, b.Position())
	}()

	return ch
}
