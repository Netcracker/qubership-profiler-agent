package pipe

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

func SuspendPipeReader(ctx context.Context, b *PipeReader) <-chan SuspendItem {
	ch := make(chan SuspendItem)

	go func() {
		defer close(ch)

		var err error
		lines := 0
		phrases := 0

		lengthOfPhrase := -1
		sTime := uint64(0)
		cTime := uint64(0)
		for !b.EOF() {
			if lengthOfPhrase <= 0 {
				lengthOfPhrase, err = b.ReadFixedInt(ctx)
				if err != nil || b.EOF() || lengthOfPhrase < 0 {
					break
				}
				phrases++
				if sTime == 0 {
					sTime, err = b.ReadFixedLong(ctx)
					if err != nil || b.EOF() || sTime < 0 {
						break
					}
					cTime = sTime
				}
			}

			pos := b.Position()
			sDt, err := b.ReadVarInt(ctx)
			if b.EOF() || err != nil {
				break
			}
			sDelay, err := b.ReadVarInt(ctx)
			if b.EOF() || err != nil {
				break
			}
			cTime += uint64(sDt)
			lengthOfPhrase -= int(b.Position() - pos)

			ts := time.UnixMilli(int64(cTime)).UTC()

			ch <- SuspendItem{
				id:     lines,
				Amount: sDelay,
				Time:   ts,
				delta:  sDt,
				pos:    pos,
			}

			lines++
		}

		log.Debug(ctx, " * read: EOF. %d lines, %d phrases, %d model bytes ", lines, phrases, b.Position())
		startTime := time.UnixMilli(int64(sTime)).UTC()
		endTime := time.UnixMilli(int64(cTime)).UTC()
		log.Debug(ctx, "  * start time: %v - %v", sTime, startTime.Format(time.RFC3339Nano))
		log.Debug(ctx, "  * end   time: %v - %v", cTime, endTime.Format(time.RFC3339Nano))
	}()

	return ch
}
