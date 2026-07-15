// Package calltree turns a per-call trace blob into the server-decoded call
// tree of 02-read-contract.md §2.5 and encodes it as MessagePack with
// int-keyed maps and a version envelope (§2.5.2-§2.5.4). Both tiers render
// through this one package — the hot path resolves big-parameter references
// against the replica's value segments, the cold path against the values the
// seal pass inlined into the parquet row — so the wire shape cannot drift
// between them.
package calltree

import (
	"encoding/binary"
	"fmt"

	"github.com/Netcracker/qubership-profiler-backend/libs/parser/pipe"
	"github.com/pkg/errors"
)

type (
	// Tree is the §2.5.2 response body before encoding: the root node plus the
	// per-tree dictionaries carrying only the strings this tree references.
	Tree struct {
		Methods []string
		Params  []string
		Root    *Node
	}

	// Node is one call in the tree (§2.5.3). Times are milliseconds relative
	// to the root's enter, reconstructed as timerStartTime + Σ(event deltas)
	// within each chunk (01-write-contract.md §4.2).
	Node struct {
		MethodIdx  int
		EnterMsRel int64
		DurationMs int64
		Params     []Param
		Children   []*Node
	}

	// Param is one parameter of a node (§2.5.3). Values keeps the tag-event
	// order. Unresolved lists the indexes of values whose big-parameter
	// reference could not be resolved; such a value carries the textual
	// reference instead of the payload, so nothing is lost silently.
	Param struct {
		ParamIdx   int
		Values     []string
		Unresolved []int
	}

	// Options resolve the blob's dictionary ids and big-parameter references.
	Options struct {
		// Dict resolves a dictionary id to its word. A missing word renders as
		// the "#<id>" placeholder, matching the /calls list path.
		Dict func(id int) (string, bool)
		// BigValue resolves a (stream, rolling_seq, offset) reference into the
		// sql / xml value streams (01-write-contract.md §4.4). false marks the
		// value unresolved. Nil treats every reference as unresolved.
		BigValue func(stream string, seq int, offset int64) (string, bool)
	}

	// BigRef is one big-parameter reference found in a blob.
	BigRef struct {
		Stream string // "sql" (PARAM_BIG_DEDUP) or "xml" (PARAM_BIG)
		Seq    int
		Offset int64
	}
)

// String renders the "<stream>:<seq>:<offset>" form shared by the seal-pass
// big_params column, the internal values endpoint, and the unresolved marker.
func (r BigRef) String() string {
	return fmt.Sprintf("%s:%d:%d", r.Stream, r.Seq, r.Offset)
}

// CollectBigRefs walks the blob and returns the call's big-parameter
// references in event order — tail and head noise excluded. The hot path uses
// it to fetch every value from the replica in one round-trip before Build.
func CollectBigRefs(blob []byte, recordIndex int) ([]BigRef, error) {
	refs := []BigRef{}
	err := walkCall(blob, recordIndex, func(ev event, _ int64) {
		if ev.kind == pipe.EventTagRecord && isBigParam(ev.paramType) {
			refs = append(refs, ev.bigRef())
		}
	})
	return refs, err
}

func isBigParam(paramType byte) bool {
	return int(paramType) == pipe.ParamBig || int(paramType) == pipe.ParamBigDedup
}

// bigRef maps a tag event to its value-stream reference, mirroring the seal
// pass (PARAM_BIG_DEDUP → sql, PARAM_BIG → xml; 01-write-contract.md §4.4).
func (ev event) bigRef() BigRef {
	stream := "xml"
	if int(ev.paramType) == pipe.ParamBigDedup {
		stream = "sql"
	}
	return BigRef{Stream: stream, Seq: ev.bigSeq, Offset: ev.bigOffset}
}

// Build decodes the blob into the call tree. recordIndex is the PK component
// locating the root ENTER within the blob's first chunk (01-write-contract.md
// §4.5).
func Build(blob []byte, recordIndex int, opts Options) (*Tree, error) {
	b := &builder{
		opts:      opts,
		tree:      &Tree{Methods: []string{}, Params: []string{}},
		methodIdx: map[string]int{},
		paramIdx:  map[string]int{},
	}
	if err := walkCall(blob, recordIndex, b.visit); err != nil {
		return nil, err
	}
	return b.tree, nil
}

// builder folds the call's events into the tree.
type builder struct {
	opts      Options
	tree      *Tree
	methodIdx map[string]int
	paramIdx  map[string]int
	stack     []*Node
	rootEnter int64
}

func (b *builder) visit(ev event, atMs int64) {
	switch ev.kind {
	case pipe.EventEnterRecord:
		node := &Node{MethodIdx: b.internMethod(ev.tagId)}
		if b.tree.Root == nil {
			b.tree.Root = node
			b.rootEnter = atMs
		} else {
			parent := b.stack[len(b.stack)-1]
			parent.Children = append(parent.Children, node)
		}
		node.EnterMsRel = atMs - b.rootEnter
		b.stack = append(b.stack, node)
	case pipe.EventExitRecord:
		node := b.stack[len(b.stack)-1]
		node.DurationMs = atMs - b.rootEnter - node.EnterMsRel
		b.stack = b.stack[:len(b.stack)-1]
	case pipe.EventTagRecord:
		node := b.stack[len(b.stack)-1]
		idx := b.internParam(ev.tagId)
		var param *Param
		for i := range node.Params {
			if node.Params[i].ParamIdx == idx {
				param = &node.Params[i]
				break
			}
		}
		if param == nil {
			node.Params = append(node.Params, Param{ParamIdx: idx})
			param = &node.Params[len(node.Params)-1]
		}
		if isBigParam(ev.paramType) {
			ref := ev.bigRef()
			value, ok := "", false
			if b.opts.BigValue != nil {
				value, ok = b.opts.BigValue(ref.Stream, ref.Seq, ref.Offset)
			}
			if !ok {
				// Explicit, not silent: the value slot carries the reference
				// text and Unresolved flags it (02 §2.5.3).
				param.Unresolved = append(param.Unresolved, len(param.Values))
				value = ref.String()
			}
			param.Values = append(param.Values, value)
		} else {
			param.Values = append(param.Values, ev.value)
		}
	}
}

func (b *builder) internMethod(id int) int {
	return intern(b.word(id), b.methodIdx, &b.tree.Methods)
}

func (b *builder) internParam(id int) int {
	return intern(b.word(id), b.paramIdx, &b.tree.Params)
}

func (b *builder) word(id int) string {
	if b.opts.Dict != nil {
		if w, ok := b.opts.Dict(id); ok {
			return w
		}
	}
	return fmt.Sprintf("#%d", id)
}

func intern(word string, index map[string]int, out *[]string) int {
	if i, ok := index[word]; ok {
		return i
	}
	i := len(*out)
	index[word] = i
	*out = append(*out, word)
	return i
}

// event is one decoded trace event plus its payload.
type event struct {
	kind      byte
	tagId     int
	paramType byte
	value     string
	bigSeq    int
	bigOffset int64
}

// walkCall drives visit over exactly the call's own events, applying the
// reader semantics of 01-write-contract.md §4.5: the 8-byte timer epoch, then
// the blob's chunks — full chunks of one thread. In the first chunk, events
// before recordIndex are the previous call's tail noise: their time deltas
// accumulate, their payloads are dropped. The event at recordIndex must be
// the root ENTER. The walk tracks call depth and stops at the depth-0 exit;
// anything after it is the next call's head noise and is never parsed. visit
// receives the absolute event time timerStart + Σ(deltas within the chunk)
// (§4.2; TracePodReader.java:152-179). A blob that ends before the depth-0
// exit is a storage-side error, not client input.
func walkCall(blob []byte, recordIndex int, visit func(ev event, atMs int64)) error {
	if len(blob) < 8 {
		return errors.New("blob shorter than its 8-byte timer epoch")
	}
	timerStart := int64(binary.BigEndian.Uint64(blob))
	pos := 8

	w := &callWalk{recordIndex: recordIndex, visit: visit}
	firstChunk := true
	for pos < len(blob) && !w.done {
		consumed, err := w.chunk(blob[pos:], timerStart, firstChunk)
		if err != nil {
			return err
		}
		pos += consumed
		firstChunk = false
	}
	if !w.done {
		return errors.New("blob ends before the depth-0 exit of the call")
	}
	return nil
}

// callWalk is the §4.5 span state across chunks.
type callWalk struct {
	recordIndex int
	visit       func(ev event, atMs int64)
	started     bool
	depth       int
	done        bool
}

// chunk decodes one chunk: the 16-byte [threadId, startTime] header, then
// events to EVENT_FINISH_RECORD. Within a chunk the event-time deltas
// accumulate from the timer epoch, not from the header's start time.
func (w *callWalk) chunk(chunk []byte, timerStart int64, firstChunk bool) (int, error) {
	if len(chunk) < 17 {
		return 0, errors.New("chunk shorter than its 16-byte header")
	}
	pos := 16 // threadId and startTime are framing; times derive from the epoch

	uvarint := func() (int64, error) {
		v, n := binary.Uvarint(chunk[pos:])
		if n <= 0 {
			return 0, errors.Errorf("torn varint at chunk offset %d", pos)
		}
		pos += n
		return int64(v), nil
	}

	eventMs := timerStart
	for index := 0; ; index++ {
		if pos >= len(chunk) {
			return pos, errors.New("chunk has no EVENT_FINISH_RECORD")
		}
		header := chunk[pos]
		pos++
		kind := header & 0x3
		if kind == pipe.EventFinishRecord {
			return pos, nil
		}
		delta := int64(header&0x7f) >> 2
		if header&0x80 != 0 {
			more, err := uvarint()
			if err != nil {
				return pos, err
			}
			delta |= more << 5
		}
		eventMs += delta

		ev := event{kind: kind}
		if kind != pipe.EventExitRecord {
			tagId, err := uvarint()
			if err != nil {
				return pos, err
			}
			ev.tagId = int(tagId)
		}
		if kind == pipe.EventTagRecord {
			if pos >= len(chunk) {
				return pos, errors.New("torn tag param type")
			}
			ev.paramType = chunk[pos]
			pos++
			switch int(ev.paramType) {
			case pipe.ParamInline, pipe.ParamIndex:
				runes, err := uvarint()
				if err != nil {
					return pos, err
				}
				if pos+2*int(runes) > len(chunk) {
					return pos, errors.New("torn tag string value")
				}
				value := make([]rune, runes)
				for i := range value {
					// The agent writes 2-byte chars; mirror the pipe reader.
					value[i] = rune(int16(binary.BigEndian.Uint16(chunk[pos+2*i:])))
				}
				pos += 2 * int(runes)
				ev.value = string(value)
			case pipe.ParamBig, pipe.ParamBigDedup:
				seq, err := uvarint()
				if err != nil {
					return pos, err
				}
				off, err := uvarint()
				if err != nil {
					return pos, err
				}
				ev.bigSeq, ev.bigOffset = int(seq), off
			default:
				return pos, errors.Errorf("unknown tag param type %d", ev.paramType)
			}
		}

		if !w.started {
			if firstChunk && index < w.recordIndex {
				continue // tail noise: time advanced, payload skipped
			}
			if kind != pipe.EventEnterRecord {
				return pos, errors.Errorf("record_index %d does not land on an ENTER", w.recordIndex)
			}
			w.started = true
		}

		switch kind {
		case pipe.EventEnterRecord:
			w.depth++
		case pipe.EventExitRecord:
			w.depth--
		}
		w.visit(ev, eventMs)
		if w.depth == 0 {
			w.done = true
			return pos, nil // head noise past the depth-0 exit is never parsed
		}
	}
}
