package streams

import (
	"context"
	"fmt"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

func ReadParams(ctx context.Context, c *model.Chunk) (*data.Params, string, error) {
	var err error
	stream := "params"
	b := AsBlob(c)
	log.Debug(ctx, " * reading '%s': %s", stream, c.String())

	b.PrintDebug(ctx)
	hGram := &Histogram{}
	i := 0 // lines
	j := 0 // phrases
	strLen := 0
	var s strings.Builder

	parsed := &data.Params{
		List: []data.Param{},
	}

	lengthOfPhrase := -1
	version := uint8(0)
	for !b.EOF() {
		i++
		if lengthOfPhrase <= 0 {
			lengthOfPhrase, err = b.ReadFixedInt(ctx)
			if err != nil || b.EOF() {
				break
			}
			j++
			if version == 0 {
				version, err = b.ReadFixedByte(ctx)
				if err != nil || b.EOF() {
					break
				}
			}
		}
		pos := int(b.Pos())

		_, _, pName := b.ReadVarString(ctx)
		var res byte
		res, err = b.ReadFixedByte(ctx)
		pIndex := res == 1
		res, err = b.ReadFixedByte(ctx)
		pList := res == 1
		pOrder := b.ReadVarInt(ctx)
		_, _, pSignature := b.ReadVarString(ctx)

		cnt := int(b.Pos()) - pos
		log.Trace(ctx, "%d: '%v'{%v, %v, %v, '%s'} [%d/%d]",
			i, pName, pIndex, pList, pOrder, pSignature, len(pName)+len(pSignature), cnt)
		lengthOfPhrase -= cnt
		s.WriteString(fmt.Sprintf("%v [%v,%v,%v,%v]\r\n", pName, pIndex, pList, pOrder, pSignature))
		param := data.Param{
			Pos: pos, Bytes: cnt,
			Name:    pName,
			IsIndex: pIndex, IsList: pList, Order: pOrder,
			Signature: pSignature,
		}
		parsed.List = append(parsed.List, param)

		strLen += len(pName) + len(pSignature)
		hGram.Mark(len(pName))
		hGram.Mark(len(pSignature))
	}

	log.Debug(ctx, "  * version: %v ", version)
	log.Debug(ctx, "  * histogram of lengths: %v ", hGram)
	log.Debug(ctx, " * read '%s': EOF. %d lines, %d phrases, %d string bytes of %d model bytes (%d overhead) ",
		stream, i, j, 2*strLen, c.Size(), c.Size()-2*strLen)
	return parsed, s.String(), err
}
