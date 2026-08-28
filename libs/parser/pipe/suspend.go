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
					// The 8-byte base-time header sits inside the first phrase's
					// byte count, so charge it against lengthOfPhrase. Without
					// this the reader over-runs the first phrase by 8 bytes and
					// decodes the next phrase's length prefix as varint record
					// data (the agent emits one suspend phrase per flush window
					// that saw a pause, so any long run is multi-phrase — №4).
					lengthOfPhrase -= 8
				}
				// A header-only phrase (the base time with no pauses in the
				// window) leaves nothing to read; loop back for the next phrase
				// prefix instead of parsing the header bytes as a record.
				if lengthOfPhrase <= 0 {
					continue
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
