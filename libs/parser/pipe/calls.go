package pipe

import (
	"context"
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

// Corruption ceilings for one calls record's params. The wire defines no
// per-record param cardinality (Hotspot.MAX_PARAMS = 256 in 02-read-contract.md
// §2.5.3 is a read-path top-N container, not an encoding limit), so a desynced or
// hostile stream can carry an arbitrary count/size varint. These bounds keep a
// single corrupt record from panicking (make([]string, -1)) or exhausting memory
// (a multi-GB slice or an unbounded run of decoded strings); they sit orders of
// magnitude above any real record.
const (
	// maxCallParams caps nParams — the number of param map entries per record.
	maxCallParams = 1 << 12 // 4096
	// maxCallRecordValues caps the summed value count across a record's params,
	// bounding the []string pre-allocation regardless of how values split.
	maxCallRecordValues = 1 << 20
	// maxCallRecordParamBytes caps the wire bytes one record's params may consume.
	// It is checked after each string decodes, so it is a cumulative cap that one
	// in-flight string (up to ~20 MiB, ReadVarString's 10*1024*1024 code-unit
	// limit) can overshoot; it still bounds the decoded-string memory a record
	// can pin.
	maxCallRecordParamBytes = 1 << 24 // 16 MiB
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
		// The wire stores each record's start as a zig-zag delta from the previous
		// record, seeded by the file header (01-write-contract.md §5.1). Accumulate
		// to recover the absolute time; startTime is the header base_ms.
		callTimeMs := int64(startTime)
	records:
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
				if err != nil {
					break
				}
				dst.Method = iVal
				iVal, err = b.ReadVarInt(ctx)
				if err != nil {
					break
				}
				dst.Duration = iVal
				iVal, err = b.ReadVarInt(ctx)
				if err != nil {
					break
				}
				dst.Calls = data.LCounter(iVal)
				iVal, err = b.ReadVarInt(ctx)
				if err != nil {
					break
				}
				threadIndex := iVal
				if threadIndex == len(threadNames) {
					slen, _, s := b.ReadVarString(ctx)
					if slen < 0 || b.EOF() {
						break
					}
					threadNames = append(threadNames, s)
				}
				if threadIndex >= 0 && threadIndex < len(threadNames) {
					dst.ThreadName = threadNames[threadIndex]
				} else { // corrupt/desynced index (negative or past the thread table)
					dst.ThreadName = fmt.Sprintf("unknown # %d", threadIndex)
				}

				iVal, err = b.ReadVarInt(ctx)
				if err != nil {
					break
				}
				dst.LogsWritten = data.LBytes(iVal)
				iVal, err = b.ReadVarInt(ctx)
				if err != nil {
					break
				}
				dst.LogsGenerated = data.LBytes(iVal) + dst.LogsWritten
				iVal, err = b.ReadVarInt(ctx)
				if err != nil {
					break
				}
				dst.TraceFileIndex = iVal
				iVal, err = b.ReadVarInt(ctx)
				if err != nil {
					break
				}
				dst.BufferOffset = iVal
				iVal, err = b.ReadVarInt(ctx)
				if err != nil {
					break
				}
				dst.RecordIndex = iVal
			}
			if fileFormat >= 2 {
				lVal, err := b.ReadVarLong(ctx)
				if err != nil {
					break
				}
				dst.CpuTime = lVal
				lVal, err = b.ReadVarLong(ctx)
				if err != nil {
					break
				}
				dst.WaitTime = lVal
				lVal, err = b.ReadVarLong(ctx)
				if err != nil {
					break
				}
				dst.MemoryUsed = lVal
			}
			if fileFormat >= 3 {
				lVal, err := b.ReadVarLong(ctx)
				if err != nil {
					break
				}
				dst.FileRead = data.LBytes(lVal)
				lVal, err = b.ReadVarLong(ctx)
				if err != nil {
					break
				}
				dst.FileWritten = data.LBytes(lVal)
				lVal, err = b.ReadVarLong(ctx)
				if err != nil {
					break
				}
				dst.NetRead = data.LBytes(lVal)
				lVal, err = b.ReadVarLong(ctx)
				if err != nil {
					break
				}
				dst.NetWritten = data.LBytes(lVal)
			}
			if fileFormat >= 4 {
				iVal, err := b.ReadVarInt(ctx)
				if err != nil {
					break
				}
				dst.Transactions = data.LCounter(iVal)
				iVal, err = b.ReadVarInt(ctx)
				if err != nil {
					break
				}
				dst.QueueWaitDuration = iVal
			}
			// read params
			nParams, err := b.ReadVarInt(ctx)
			if err != nil {
				break
			}
			if nParams < 0 || nParams > maxCallParams {
				b.setErr(fmt.Errorf("calls: nParams %d out of range [0,%d] at pos %d",
					nParams, maxCallParams, b.Position()))
				break
			}
			if nParams > 0 {
				dst.Params = map[data.TagId][]string{}
				// paramsStart anchors the per-record param byte budget; the
				// running recordValues bounds the summed []string allocation.
				paramsStart := b.Position()
				recordValues := 0
				for i := 0; i < nParams; i++ {
					paramId, err := b.ReadVarInt(ctx)
					if err != nil {
						break records
					}
					size, err := b.ReadVarInt(ctx)
					if err != nil {
						break records
					}
					if size < 0 || recordValues+size > maxCallRecordValues {
						b.setErr(fmt.Errorf("calls: param value count %d (running %d) out of range [0,%d] at pos %d",
							size, recordValues, maxCallRecordValues, b.Position()))
						break records
					}
					recordValues += size
					if size == 0 {
						dst.Params[paramId] = []string{}
					} else if size == 1 {
						slen, _, ps := b.ReadVarString(ctx)
						if slen < 0 || b.EOF() {
							break records
						}
						if b.Position()-paramsStart > maxCallRecordParamBytes {
							b.setErr(fmt.Errorf("calls: record params exceed %d bytes at pos %d",
								maxCallRecordParamBytes, b.Position()))
							break records
						}
						dst.Params[paramId] = []string{ps}
					} else {
						result := make([]string, size)
						for j := size - 1; j >= 0; j-- {
							slen, _, ps := b.ReadVarString(ctx)
							if slen < 0 || b.EOF() {
								break records
							}
							if b.Position()-paramsStart > maxCallRecordParamBytes {
								b.setErr(fmt.Errorf("calls: record params exceed %d bytes at pos %d",
									maxCallRecordParamBytes, b.Position()))
								break records
							}
							result[j] = ps
						}
						dst.Params[paramId] = result
					}
				}
			}

			callTimeMs += int64(dst.Time)
			cTime := time.UnixMilli(callTimeMs)
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
