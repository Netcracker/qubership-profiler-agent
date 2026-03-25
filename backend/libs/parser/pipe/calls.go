package pipe

import (
	"context"
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

func CallsPipeReader(ctx context.Context, b *PipeReader) <-chan CallItem {
	ch := make(chan CallItem)

	go func() {
		defer close(ch)

		var err error
		lines := 0

		startTime, err := b.ReadFixedLong(ctx)
		if err != nil {
			return
		}

		fileFormat := uint64(0)
		if startTime>>32 == 0xFFFEFDFC {
			fileFormat = startTime & 0xffffffff
			startTime, err = b.ReadFixedLong(ctx)
			if err != nil {
				return
			}
		}
		log.Debug(ctx, " * stream format: %v ", fileFormat)
		log.Debug(ctx, " * start time: %v -  %v", startTime, time.UnixMilli(int64(startTime)).UTC().String())

		threadNames := []string{}
		for !b.EOF() {
			if ctx.Err() != nil {
				return
			}

			pos := b.Position()
			dst := data.Call{}

			if fileFormat >= 1 {
				iVal, err := b.ReadVarIntZigZag(ctx)
				if err != nil {
					break
				}
				dst.Time = data.LTime(iVal)
				iVal, err = b.ReadVarInt(ctx)
				dst.Method = iVal
				iVal, err = b.ReadVarInt(ctx)
				dst.Duration = iVal
				iVal, err = b.ReadVarInt(ctx)
				dst.Calls = data.LCounter(iVal)
				iVal, err = b.ReadVarInt(ctx)
				threadIndex := iVal
				if threadIndex == len(threadNames) {
					_, _, s := b.ReadVarString(ctx)
					threadNames = append(threadNames, s)
				}
				if len(threadNames) > threadIndex {
					dst.ThreadName = threadNames[threadIndex]
				} else { // in case of zip errors thread index may be larger than number of threads
					dst.ThreadName = fmt.Sprintf("unknown # %d", threadIndex)
				}

				iVal, err = b.ReadVarInt(ctx)
				dst.LogsWritten = data.LBytes(iVal)
				iVal, err = b.ReadVarInt(ctx)
				dst.LogsGenerated = data.LBytes(iVal) + dst.LogsWritten
				iVal, err = b.ReadVarInt(ctx)
				dst.TraceFileIndex = iVal
				iVal, err = b.ReadVarInt(ctx)
				dst.BufferOffset = iVal
				iVal, err = b.ReadVarInt(ctx)
				dst.RecordIndex = iVal
			}
			if fileFormat >= 2 {
				lVal, err := b.ReadVarLong(ctx)
				if err != nil {
					break
				}
				dst.CpuTime = lVal
				lVal, err = b.ReadVarLong(ctx)
				dst.WaitTime = lVal
				lVal, err = b.ReadVarLong(ctx)
				dst.MemoryUsed = lVal
			}
			if fileFormat >= 3 {
				lVal, err := b.ReadVarLong(ctx)
				if err != nil {
					break
				}
				dst.FileRead = data.LBytes(lVal)
				lVal, err = b.ReadVarLong(ctx)
				dst.FileWritten = data.LBytes(lVal)
				lVal, err = b.ReadVarLong(ctx)
				dst.NetRead = data.LBytes(lVal)
				lVal, err = b.ReadVarLong(ctx)
				dst.NetWritten = data.LBytes(lVal)
			}
			if fileFormat >= 4 {
				iVal, err := b.ReadVarInt(ctx)
				if err != nil {
					break
				}
				dst.Transactions = data.LCounter(iVal)
				iVal, err = b.ReadVarInt(ctx)
				dst.QueueWaitDuration = iVal
			}
			// read params
			nParams, err := b.ReadVarInt(ctx)
			if err != nil {
				break
			}
			if nParams > 0 {
				dst.Params = map[data.TagId][]string{}
				for i := 0; i < nParams; i++ {
					iVal, err := b.ReadVarInt(ctx)
					if err != nil {
						break
					}
					paramId := iVal
					iVal, err = b.ReadVarInt(ctx)
					size := iVal
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
			ch <- CallItem{
				id:   lines,
				Time: cTime,
				Call: dst,
				pos:  pos,
			}
			lines++
		} // for
	}()

	return ch
}
