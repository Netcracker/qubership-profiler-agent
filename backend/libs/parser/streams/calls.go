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

func ReadCalls(ctx context.Context, c *model.Chunk) (*data.Calls, string, error) {
	stream := "calls"
	log.Debug(ctx, " * reading '%s': %s", stream, c.String())

	read := make(chan *data.CallInfo)
	errors := make(chan error)

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go ReadCallStream(subCtx, c, read, errors)

	parsed := &data.Calls{
		List:        []*data.CallInfo{},
		RequiredIds: map[data.TagId]bool{},
	}

	for {
		done := false
		select {
		case call, ok := <-read:
			if ok {
				parsed.List = append(parsed.List, call)
			} else {
				done = true
			}
		case err := <-errors:
			cancel()
			return nil, "", err
		}
		if done {
			break
		}
	}

	var s strings.Builder
	s.WriteString(data.CallsCsvHeader())
	for i, call := range parsed.List {
		parsed.RequiredIds[call.Call.Method] = true
		for k, _ := range call.Call.Params {
			parsed.RequiredIds[k] = true
		}
		log.ExtraTrace(ctx, "call #%d . time: %v | read %d bytes | %v ", i+1, call.Time.Format(time.RFC3339), call.Bytes, call.Call)
		s.WriteString(fmt.Sprintf("call#%d: %v\n", i+1, call.Call.Csv()))
	}

	log.Debug(ctx, " * read '%s': EOF. %d calls, %d model bytes ", stream, len(parsed.List), c.Size())
	return parsed, s.String(), nil
}

func ReadCallStream(ctx context.Context, c *model.Chunk, streamA chan<- *data.CallInfo, errors chan<- error) {
	b := AsBlob(c)

	b.PrintDebug(ctx)

	startTime, err := b.ReadFixedLong(ctx)
	if err != nil {
		errors <- err
		return
	}
	b.PrintDebug(ctx)

	fileFormat := uint64(0)
	if startTime>>32 == 0xFFFEFDFC {
		fileFormat = startTime & 0xffffffff
		startTime, err = b.ReadFixedLong(ctx)
		if err != nil {
			errors <- err
			return
		}
		b.PrintDebug(ctx)
	}
	log.Debug(ctx, " * file format: %v ", fileFormat)
	log.Debug(ctx, " * start time: %v -  %v", startTime, time.UnixMilli(int64(startTime)).UTC().String())

	threadNames := []string{}
	for !b.EOF() {
		if ctx.Err() != nil {
			return
		}

		pos := int(b.Pos())
		dst := data.Call{}

		b.PrintDebug(ctx)
		if fileFormat >= 1 {
			dst.Time = data.LTime(b.ReadVarIntZigZag(ctx))
			dst.Method = b.ReadVarInt(ctx)
			dst.Duration = b.ReadVarInt(ctx)
			dst.Calls = data.LCounter(b.ReadVarInt(ctx))
			threadIndex := b.ReadVarInt(ctx)
			if threadIndex == len(threadNames) {
				_, _, s := b.ReadVarString(ctx)
				threadNames = append(threadNames, s)
			}
			if len(threadNames) > threadIndex {
				dst.ThreadName = threadNames[threadIndex]
			} else { // in case of zip errors thread index may be larger than number of threads
				dst.ThreadName = fmt.Sprintf("unknown # %d", threadIndex)
			}

			dst.LogsWritten = data.LBytes(b.ReadVarInt(ctx))
			dst.LogsGenerated = data.LBytes(b.ReadVarInt(ctx)) + dst.LogsWritten
			dst.TraceFileIndex = b.ReadVarInt(ctx)
			dst.BufferOffset = b.ReadVarInt(ctx)
			dst.RecordIndex = b.ReadVarInt(ctx)
		}
		if fileFormat >= 2 {
			dst.CpuTime = b.ReadVarLong(ctx)
			dst.WaitTime = b.ReadVarLong(ctx)
			dst.MemoryUsed = b.ReadVarLong(ctx)
		}
		if fileFormat >= 3 {
			dst.FileRead = data.LBytes(b.ReadVarLong(ctx))
			dst.FileWritten = data.LBytes(b.ReadVarLong(ctx))
			dst.NetRead = data.LBytes(b.ReadVarLong(ctx))
			dst.NetWritten = data.LBytes(b.ReadVarLong(ctx))
		}
		if fileFormat >= 4 {
			dst.Transactions = data.LCounter(b.ReadVarInt(ctx))
			dst.QueueWaitDuration = b.ReadVarInt(ctx)
		}
		// read params
		if nParams := b.ReadVarInt(ctx); nParams > 0 {
			dst.Params = map[data.TagId][]string{}
			for i := 0; i < nParams; i++ {
				paramId := b.ReadVarInt(ctx)
				size := b.ReadVarInt(ctx)
				if size == 0 {
					dst.Params[paramId] = []string{}
				} else if size == 1 {
					_, _, ps := b.ReadVarString(ctx)
					dst.Params[paramId] = []string{ps}
				} else {
					result := make([]string, size)
					for size--; size >= 0; size-- {
						_, _, ps := b.ReadVarString(ctx)
						result[size] = ps
					}
					dst.Params[paramId] = result
				}
			}
		}

		cTime := time.UnixMilli(int64(startTime) + dst.Time)
		cnt := int(b.Pos()) - pos

		streamA <- &data.CallInfo{Pos: pos, Bytes: cnt, Time: cTime, Call: dst}
	}
	close(streamA)

	return
}
