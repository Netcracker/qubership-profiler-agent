package streams

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

func ReadSuspend(ctx context.Context, c *model.Chunk) (*data.Suspends, string, error) {
	var err error
	stream := "suspend"
	b := AsBlob(c)
	log.Debug(ctx, " * reading '%s': %s", stream, c.String())

	b.PrintDebug(ctx)
	i := 0 // lines
	j := 0 // phrases
	var s strings.Builder

	parsed := &data.Suspends{
		List: []data.Suspend{},
	}

	phrases := map[int]bool{}

	lengthOfPhrase := -1
	sTime := uint64(0)
	cTime := uint64(0)
	for !b.EOF() {
		i++
		if lengthOfPhrase <= 0 {
			lengthOfPhrase, err = b.ReadFixedInt(ctx)
			if err != nil || b.EOF() {
				break
			}
			j++
			log.Trace(ctx, "%d: %v ... new phrase ... %d [%X]", i, cTime, lengthOfPhrase, lengthOfPhrase)
			phrases[lengthOfPhrase] = true
			if sTime == 0 {
				sTime, err = b.ReadFixedLong(ctx)
				if err != nil || b.EOF() {
					break
				}
				cTime = sTime
			}
		}
		pos := int(b.Pos())
		sDt := b.ReadVarInt(ctx)
		sDelay := b.ReadVarInt(ctx)
		cnt := int(b.Pos()) - pos
		cTime += uint64(sDt)
		lengthOfPhrase -= cnt
		log.Trace(ctx, "%d: %v {%v, %v} [%d] ... %d", i, cTime, sDt, sDelay, cnt, lengthOfPhrase)

		ts := time.UnixMilli(int64(cTime)).UTC()
		suspend := data.Suspend{
			Pos: pos, Bytes: cnt,
			Time:   ts,
			Delta:  sDt,
			Amount: sDelay,
		}
		parsed.List = append(parsed.List, suspend)
		s.WriteString(fmt.Sprintf("%v:  %v, %v  \t %v\n", cTime, sDt, sDelay, ts))
	}

	parsed.StartTime = time.UnixMilli(int64(sTime)).UTC()
	parsed.EndTime = time.UnixMilli(int64(cTime)).UTC()

	log.Debug(ctx, "  * phrases %v ", phrases)
	log.Debug(ctx, "  * start time: %v - %v", sTime, parsed.StartTime.Format(time.RFC3339Nano))
	log.Debug(ctx, "  * end   time: %v - %v", cTime, parsed.EndTime.Format(time.RFC3339Nano))
	log.Debug(ctx, " * read '%s': EOF. %d lines, %d phrases, %d model bytes ",
		stream, i, j, c.Size())
	return parsed, s.String(), err
}
