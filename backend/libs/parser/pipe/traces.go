package pipe

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

const (
	EventEnterRecord  = byte(0)
	EventExitRecord   = byte(1)
	EventTagRecord    = byte(2)
	EventFinishRecord = byte(3)
	ParamInline       = 0
	ParamIndex        = 2
	ParamBig          = 1
	ParamBigDedup     = 1 | 2

	TimeMsOnly = "15:04:05.000"
	//TagsCallActive = int(-4)
)

func rtTime(realTime uint64) string {
	tsTime := time.UnixMilli(int64(realTime))
	return fmt.Sprintf("%v - %v", realTime, tsTime.UTC())
}

func TracesPipeReader(ctx context.Context, b *PipeReader) <-chan TraceItem {
	ch := make(chan TraceItem)

	go func() {
		defer close(ch)

		var err error
		lines := 0
		timerStartTime, err := b.ReadFixedLong(ctx)
		if err != nil || b.EOF() {
			return
		}
		headerSize := len(b.GetAndEmptyBuffer())
		log.Debug(ctx, " * start time: %v -  %v [%d bytes]",
			timerStartTime, time.UnixMilli(int64(timerStartTime)).UTC().String(), headerSize)

		for !b.EOF() {
			pos := b.Position()
			var s strings.Builder

			threadId, err := b.ReadFixedLong(ctx)
			if err != nil || b.EOF() {
				break
			}
			realTime, err := b.ReadFixedLong(ctx)
			if err != nil || b.EOF() {
				break
			}
			trRecordTime := time.UnixMilli(int64(realTime))
			s.WriteString(fmt.Sprintf("\nblock #%d. threadId=%4d, real time: %v , offset=%d / %X\n",
				lines, threadId, rtTime(realTime), pos, pos))

			tagIds := map[int]bool{}

			realTimeOffset := int(realTime - timerStartTime)
			eventTime := -realTimeOffset
			j := 0
			sp := 0
			for !b.EOF() {
				j++

				header, err := b.ReadFixedByte(ctx)
				if err != nil || b.EOF() {
					break
				}
				typ := header & 0x3

				if typ == EventFinishRecord {
					log.ExtraTrace(ctx, " * [%d:%d] got EVENT_FINISH_RECORD=%v", lines, j, EventFinishRecord)
					break
				}
				etime := int(header&0x7f) >> 2
				if (header & 0x80) > 0 {
					iVal, err := b.ReadVarInt(ctx)
					if err != nil || b.EOF() {
						break
					}
					etime = etime | iVal<<5
				}
				eventTime += etime

				tagId := 0
				if typ != EventExitRecord {
					tagId, err = b.ReadVarInt(ctx)
					if err != nil || b.EOF() {
						break
					}

					if typ == EventTagRecord {
						paramType, err := b.ReadFixedByte(ctx)
						if err != nil || b.EOF() {
							break
						}
						switch paramType {
						case ParamIndex, ParamInline:
							_, _, value := b.ReadVarString(ctx)
							s.WriteString(fmt.Sprintf("trace [%3d:%2d] tagId=%v, string value '%v'\n", lines, j-1, tagId, value))
							tagIds[tagId] = true
							break
						case ParamBigDedup, ParamBig:
							traceIndex, err := b.ReadVarInt(ctx)
							if err != nil || b.EOF() {
								break
							}
							offs, err := b.ReadVarInt(ctx)
							if err != nil || b.EOF() {
								break
							}
							s.WriteString(fmt.Sprintf("trace [%3d:%2d] tagId=%v, clob: traceIdx=%v, offset=%v\n", lines, j-1, tagId, traceIndex, offs))
							tagIds[tagId] = true
							break
						}
					}
				}

				switch typ {
				case EventEnterRecord:
					sp++
					if sp == 1 {
						s.WriteString(fmt.Sprintf("call  [%3d:%2d] tagId=%v\n", lines, j-1, tagId))
					} else {
						s.WriteString(fmt.Sprintf("call  [%3d:%2d] %s -> tagId=%v\n", lines, j-1, repeat(" | ", sp-1), tagId))
					}
					break
				case EventExitRecord:
					if sp == 1 {
						s.WriteString(fmt.Sprintf("call  [%3d:%2d] tagId=%v\n", lines, j-1, tagId))
					} else {
						if sp == 0 {
							s.WriteString(fmt.Sprintf("ERROR [%3d:%2d] %s [tagId=%d]\n", lines, j-1, repeat(" | ", sp), tagId))
						} else {
							s.WriteString(fmt.Sprintf("call  [%3d:%2d] %s <- tagId=%v\n", lines, j-1, repeat(" | ", sp-1), tagId))
						}
					}
					if sp > 0 {
						sp--
					}
					break
				}
			}
			nBytes := int(b.Position() - pos)
			log.Trace(ctx, "trace #%d. threadId: %4d, real time: %v | %4d tag lines, %4d uniq tags, read %5d bytes, start offset=[%d / %X]",
				lines, threadId, rtTime(realTime), j, len(tagIds), nBytes, pos, pos)

			ch <- TraceItem{
				Id:     lines,
				Offset: pos,
				Time:   trRecordTime,
				bytes:  nBytes,
				Data:   b.GetAndEmptyBuffer(), // TODO check performance
				debug:  s.String(),
			}
			lines++
		}
		log.Debug(ctx, " * read EOF. %d trace records, %d model bytes ", lines, b.Position())
	}()

	return ch
}

func repeat(s string, count int) string {
	if count <= 0 {
		return fmt.Sprintf("[?|%d]", count)
	}
	return strings.Repeat(s, count)
}
