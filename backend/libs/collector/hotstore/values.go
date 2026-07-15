package hotstore

// Big-parameter value resolution against the sql / xml value segments
// (01-write-contract.md §4.4). A PARAM_BIG / PARAM_BIG_DEDUP trace tag points
// at a var-string — a varint char count followed by 2-byte chars — inside the
// decompressed stream file; the reference's rolling_seq is the segment name
// and its offset is the var-string's first byte. The seal pass resolves the
// references it met during blob assembly and inlines the values into the
// parquet row (the value segments never reach S3); the internal values
// endpoint resolves them live for the hot /tree path.

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/pkg/errors"
)

// ValueRef locates one big-parameter value in a hot-store value segment.
type ValueRef struct {
	Stream string // StreamSql or StreamXml
	Seq    int
	Offset int64
}

// String renders the "<stream>:<seq>:<offset>" form shared with the sealed
// big_params_json keys and the internal values endpoint.
func (r ValueRef) String() string {
	return fmt.Sprintf("%s:%d:%d", r.Stream, r.Seq, r.Offset)
}

// ParseValueRef inverts ValueRef.String.
func ParseValueRef(s string) (ValueRef, error) {
	var r ValueRef
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return r, errors.Errorf("value ref %q: expected <stream>:<seq>:<offset>", s)
	}
	r.Stream = parts[0]
	if r.Stream != StreamSql && r.Stream != StreamXml {
		return r, errors.Errorf("value ref %q: stream must be %s or %s", s, StreamSql, StreamXml)
	}
	seq, err := strconv.Atoi(parts[1])
	if err != nil {
		return r, errors.Errorf("value ref %q: bad rolling_seq", s)
	}
	r.Seq = seq
	r.Offset, err = strconv.ParseInt(parts[2], 10, 64)
	if err != nil || r.Offset < 0 || r.Seq < 0 {
		return r, errors.Errorf("value ref %q: bad offset", s)
	}
	return r, nil
}

// BigValues resolves refs against the pod-restart's value segments. The
// result maps only the references that resolved; a missing segment or a torn
// tail drops its references from the map (and logs), because a lost value
// must degrade to an explicit unresolved marker downstream, never fail the
// whole read (01-write-contract.md §4.6 spirit).
func (s *Store) BigValues(ctx context.Context, key PodRestartKey, refs []ValueRef) (map[ValueRef]string, error) {
	pr, ok := s.PodRestart(key)
	if !ok {
		return nil, errors.Errorf("unknown pod-restart %s", key)
	}
	// A live pod-restart still holds unflushed bytes in its gzip writers.
	if !pr.Closed() {
		if err := pr.FlushSegments(); err != nil {
			return nil, err
		}
	}
	return pr.readBigValues(ctx, refs), nil
}

// readBigValues reads each referenced segment once, walking its references in
// offset order over the forward-only gzip stream.
func (pr *PodRestart) readBigValues(ctx context.Context, refs []ValueRef) map[ValueRef]string {
	bySeg := map[segKey][]ValueRef{}
	for _, ref := range refs {
		sk := segKey{ref.Stream, ref.Seq}
		bySeg[sk] = append(bySeg[sk], ref)
	}

	out := make(map[ValueRef]string, len(refs))
	for sk, segRefs := range bySeg {
		sort.Slice(segRefs, func(i, j int) bool { return segRefs[i].Offset < segRefs[j].Offset })
		path := filepath.Join(pr.dir, sk.stream, SegmentFileName(sk.seq))
		reader, err := openSegmentReader(path)
		if err != nil {
			log.Warning(ctx, "big values: %s segment %d of %v is unreadable: %v", sk.stream, sk.seq, pr.Key, err)
			continue
		}
		pos := int64(0)
		for _, ref := range segRefs {
			if ref.Offset < pos {
				// An exact duplicate (PARAM_BIG_DEDUP repeats a reference) was
				// resolved by the previous iteration; anything else overlaps
				// the previous var-string, which cannot come from the agent.
				if _, ok := out[ref]; !ok {
					log.Warning(ctx, "big values: %s ref %v of %v overlaps the previous value", sk.stream, ref, pr.Key)
				}
				continue
			}
			if _, err := io.CopyN(io.Discard, reader, ref.Offset-pos); err != nil {
				log.Warning(ctx, "big values: %s segment %d of %v lost its tail: %v", sk.stream, sk.seq, pr.Key, err)
				break
			}
			value, consumed, err := readVarString(reader)
			if err != nil {
				log.Warning(ctx, "big values: %s ref %v of %v does not decode: %v", sk.stream, ref, pr.Key, err)
				break
			}
			out[ref] = value
			pos = ref.Offset + consumed
		}
		_ = reader.Close()
	}
	return out
}

// readVarString decodes one var-string from a forward-only reader: a varint
// char count, then 2-byte big-endian chars — the format of every value in the
// sql / xml streams (backend/libs/parser/pipe/strings.go). It reports the
// bytes consumed so a sequential caller can track its stream position.
func readVarString(r io.Reader) (string, int64, error) {
	var n uint64
	var shift uint
	consumed := int64(0)
	var b [1]byte
	for {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return "", consumed, errors.Wrap(err, "read var-string length")
		}
		consumed++
		n |= uint64(b[0]&0x7f) << shift
		if b[0]&0x80 == 0 {
			break
		}
		shift += 7
		if shift > 63 {
			return "", consumed, errors.New("var-string length varint overflows")
		}
	}
	raw := make([]byte, 2*n)
	if _, err := io.ReadFull(r, raw); err != nil {
		return "", consumed, errors.Wrap(err, "read var-string chars")
	}
	consumed += int64(len(raw))
	// The agent writes each char as one UTF-16 code unit; decode the run as a
	// whole so surrogate pairs reassemble into full runes (mirror
	// PipeReader.ReadVarString, not a signed per-char cast).
	units := make([]uint16, n)
	for i := range units {
		units[i] = binary.BigEndian.Uint16(raw[2*i:])
	}
	return string(utf16.Decode(units)), consumed, nil
}
