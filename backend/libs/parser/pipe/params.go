package pipe

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

func ParamsPipeReader(ctx context.Context, b *PipeReader) <-chan ParamItem {

	ch := make(chan ParamItem)

	go func() {
		defer close(ch)

		var err error
		lines := 0
		phrases := 0

		version := uint8(0)
		lengthOfPhrase := -1
		for !b.EOF() {
			if lengthOfPhrase <= 0 {
				lengthOfPhrase, err = b.ReadFixedInt(ctx)
				if err != nil || b.EOF() {
					break
				}
				phrases++
				if version == 0 {
					version, err = b.ReadFixedByte(ctx)
					if err != nil || b.EOF() {
						break
					}
				}
			}
			pos := b.Position()

			_, _, pName := b.ReadVarString(ctx)

			var res byte
			res, err = b.ReadFixedByte(ctx)
			if b.EOF() || err != nil {
				break
			}
			pIndex := res == 1

			res, err = b.ReadFixedByte(ctx)
			if b.EOF() || err != nil {
				break
			}
			pList := res == 1

			pOrder, err := b.ReadVarInt(ctx)
			if b.EOF() || err != nil {
				break
			}

			length, _, pSignature := b.ReadVarString(ctx)
			if b.EOF() || length < 0 || err != nil {
				break
			}

			lengthOfPhrase -= int(b.Position() - pos)

			ch <- ParamItem{
				Id:        lines,
				Name:      pName,
				IsIndex:   pIndex,
				IsList:    pList,
				Order:     pOrder,
				Signature: pSignature,
				pos:       pos,
			}
			lines++

		}
		log.Debug(ctx, "  * version: %v ", version)
		log.Debug(ctx, " * read EOF. %d lines, %d phrases of %d model bytes ", lines, phrases, b.Position())

	}()

	return ch
}
