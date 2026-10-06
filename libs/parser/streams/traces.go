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

func ReadTraces(ctx context.Context, c *model.Chunk, dictionary *data.Dictionary) (*data.Traces, string, error) {
	stream := "trace"
	b := AsBlob(c)
	log.Trace(ctx, " * reading '%s': %s", stream, c.String())

	i := 0 // lines
	var s strings.Builder
	parsed := &data.Traces{List: []data.TraceRecord{}}

	//b.PrintDebug(ctx)
	timerStartTime, err := b.ReadFixedLong(ctx)
	if err != nil || b.EOF() {
		return nil, "", err
	}
	log.Trace(ctx, " * start time: %v -  %v", timerStartTime, time.UnixMilli(int64(timerStartTime)).UTC().String())

	for !b.EOF() {
		i++
		pos := int(b.Pos())

		b.PrintDebug(ctx)
		threadId, err := b.ReadFixedLong(ctx)
		if err != nil || b.EOF() {
			break
		}
		realTime, err := b.ReadFixedLong(ctx)
		if err != nil || b.EOF() {
			break
		}
		trRecordTime := time.UnixMilli(int64(realTime))
		log.ExtraTrace(ctx, "trace #%d. threadId: %4d, real time: %v , offset=%d / %X",
			i, threadId, rtTime(realTime), pos, pos)
		s.WriteString(fmt.Sprintf("\nblock #%d. threadId=%4d, real time: %v , offset=%d / %X\n",
			i, threadId, rtTime(realTime), pos, pos))

		tagIds := map[int]bool{}

		realTimeOffset := int(realTime - timerStartTime)
		eventTime := -realTimeOffset
		j := 0
		sp := 0
		for !b.EOF() {
			j++

			//b.PrintDebug(ctx)
			header, err := b.ReadFixedByte(ctx)
			if err != nil || b.EOF() {
				break
			}
			typ := header & 0x3
			//utils.LogDebug(ctx, " * [%d:%d] header=%X, typ=%x", i, j, header, typ)

			if typ == EventFinishRecord {
				log.ExtraTrace(ctx, " * [%d:%d] got EVENT_FINISH_RECORD=%v", i, j, EventFinishRecord)
				break
			}
			etime := int(header&0x7f) >> 2
			if (header & 0x80) > 0 {
				//b.PrintDebug(ctx)
				etime = etime | b.ReadVarInt(ctx)<<5
				//utils.LogDebug(ctx, " * [%d:%d] etime=%v", i, j, etime)
			}
			eventTime += etime

			tagId := 0
			if typ != EventExitRecord {
				//b.PrintDebug(ctx)
				tagId = b.ReadVarInt(ctx)
				//utils.LogDebug(ctx, " * [%d:%d] typ=%v, tagId=%v", i, j, typ, tagId)

				if typ == EventTagRecord {
					//b.PrintDebug(ctx)
					paramType, err := b.ReadFixedByte(ctx)
					if err != nil || b.EOF() {
						break
					}
					//utils.LogDebug(ctx, " * [%d:%d] typ=%v, tagId=%v, paramType=%d", i, j, typ, tagId, paramType)
					switch paramType {
					case ParamIndex, ParamInline:
						//b.PrintDebug(ctx)
						_, _, value := b.ReadVarString(ctx)
						log.ExtraTrace(ctx, " . [%3d:%2d got value: tagId=%v['%v'], val=%v", i, j-1, tagId, dictionary.Get(tagId), value)
						s.WriteString(fmt.Sprintf("trace [%3d:%2d] tagId=%v, string value '%v'\n", i, j-1, tagId, value))
						tagIds[tagId] = true
						break
					case ParamBigDedup, ParamBig:
						//b.PrintDebug(ctx)
						traceIndex := b.ReadVarInt(ctx)
						offs := b.ReadVarInt(ctx)
						log.ExtraTrace(ctx, " . [%3d:%2d] got CLOB: tagId=%v, traceIdx=%v['%v'], offset=%v", i, j-1, tagId, dictionary.Get(tagId), traceIndex, offs)
						s.WriteString(fmt.Sprintf("trace [%3d:%2d] tagId=%v, clob: traceIdx=%v, offset=%v\n", i, j-1, tagId, traceIndex, offs))
						tagIds[tagId] = true
						break
					}
				}
			}

			switch typ {
			case EventEnterRecord:
				sp++
				if sp == 1 {
					log.Trace(ctx, " * [%3d:%2d] => [tagId=%d|%v] ", i, j-1, tagId, dictionary.Get(tagId))
					s.WriteString(fmt.Sprintf("call  [%3d:%2d] tagId=%v|'%v'\n", i, j-1, tagId, dictionary.Get(tagId)))
				} else {
					log.ExtraTrace(ctx, " * [%3d:%2d] %s [tagId=%d|%v] ", i, j-1, repeat(" > ", sp), tagId, dictionary.Get(tagId))
					s.WriteString(fmt.Sprintf("call  [%3d:%2d] %s -> tagId=%v|'%v'\n", i, j-1, repeat(" | ", sp-1), tagId, dictionary.Get(tagId)))
				}
				break
			case EventExitRecord:
				if sp == 1 {
					log.ExtraTrace(ctx, " * [%3d:%2d] <= [tagId=%d|%v] ", i, j-1, tagId, dictionary.Get(tagId))
					s.WriteString(fmt.Sprintf("call  [%3d:%2d] tagId=%v|'%v'\n", i, j-1, tagId, dictionary.Get(tagId)))
				} else {
					log.ExtraTrace(ctx, " * [%3d:%2d] %s [tagId=%d|%v] ", i, j-1, repeat(" < ", sp), tagId, dictionary.Get(tagId))
					if sp == 0 {
						log.Trace(ctx, " ERROR * [%3d:%2d] %s [tagId=%d|%v] - invalid exit tag, level=0 already", i, j-1, repeat(" < ", sp), tagId, dictionary.Get(tagId))
						s.WriteString(fmt.Sprintf("ERROR [%3d:%2d] %s [tagId=%d|%v]\n", i, j-1, repeat(" | ", sp), tagId, dictionary.Get(tagId)))
					} else {
						s.WriteString(fmt.Sprintf("call  [%3d:%2d] %s <- tagId=%v|'%v'\n", i, j-1, repeat(" | ", sp-1), tagId, dictionary.Get(tagId)))
					}
				}
				if sp > 0 {
					sp--
				}
				break
			}
		}
		cnt := int(b.Pos()) - pos
		log.Trace(ctx, "trace #%d. threadId: %4d, real time: %v | %4d tag lines, %4d uniq tags, read %5d bytes, start offset=[%d / %X]",
			i, threadId, rtTime(realTime), j, len(tagIds), cnt, pos, pos)

		parsed.List = append(parsed.List, data.TraceRecord{Pos: pos, Bytes: cnt, Time: trRecordTime})
		s.WriteString(fmt.Sprintf("trace %d. %d tag lines\n", i, j))

	}
	//ReadDictionary(ids);
	//readClobs(uniqueClobs.keySet());

	log.Trace(ctx, " * read '%s': EOF. %d trace records, %d model bytes ", stream, i, c.Size())
	return parsed, s.String(), err
}

func repeat(s string, count int) string {
	if count <= 0 {
		return fmt.Sprintf("[?|%d]", count)
	}
	return strings.Repeat(s, count)
}
