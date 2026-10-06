package streams

import (
	"context"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

func ReadDictionary(ctx context.Context, c *model.Chunk) (*data.Dictionary, string, error) {
	stream := "dictionary"
	log.Debug(ctx, " * reading '%s': %s", stream, c.String())

	hGram := &Histogram{}
	strLen := 0
	var s strings.Builder
	parsed, lines, phrases, err := readDictionary(ctx, c, 100000, func(b *io.BlobReader, n int, st string) bool {
		s.WriteString(st + "\r\n")
		strLen += len(st)
		hGram.Mark(len(st))
		return true
	})

	log.Debug(ctx, "  * histogram of lengths: %v ", hGram)
	log.Debug(ctx, " * read '%s': EOF. %d lines, %d phrases, %d string bytes of %d model bytes (%d overhead) ",
		stream, lines, phrases, 2*strLen, c.Size(), c.Size()-2*strLen)

	return parsed, s.String(), err
}

func ReadDictionaryUntil(ctx context.Context, c *model.Chunk, limitPhrases int, limitWords int) (*data.Dictionary, int, error) {
	// NB: return pos AFTER reading last word (from LIMIT - from 1)
	pos := 0
	parsed, _, _, err := readDictionary(ctx, c, limitPhrases, func(b *io.BlobReader, n int, st string) bool {
		if n > limitWords {
			log.Debug(ctx, "  * STOP at %d: got '%v' ", n, st)
			return false
		}
		pos = int(b.Pos())
		return true
	})
	return parsed, pos, err
}

func readDictionary(ctx context.Context, c *model.Chunk, limitPhrases int, listener func(*io.BlobReader, int, string) bool) (*data.Dictionary, int, int, error) {
	var err error
	b := AsBlob(c)
	b.PrintDebug(ctx)
	i := 0 // lines
	j := 0 // phrases

	parsed := &data.Dictionary{List: []data.DictWord{}}

	lengthOfPhrase := -1
	for !b.EOF() {
		i++
		if lengthOfPhrase <= 0 {
			j++
			if j > limitPhrases {
				break
			}

			lengthOfPhrase, err = b.ReadFixedInt(ctx)
			if err != nil || b.EOF() {
				break
			}
		}
		pos := int(b.Pos())
		_, bytes, st := b.ReadVarString(ctx)
		lengthOfPhrase -= bytes

		parsed.List = append(parsed.List, data.DictWord{Pos: pos, Bytes: int(b.Pos()) - pos, Word: st})

		if !listener(b, i, st) {
			break
		}
	}
	return parsed, i, j, err
}
