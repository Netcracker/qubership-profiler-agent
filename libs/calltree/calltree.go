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
	"math"
	"sort"
	"unicode/utf16"

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

	// Node is one merged tree node (§2.5.3): every metric comes in a
	// self/total pair. Total duration spans enter to exit as reconstructed
	// from the event deltas (01-write-contract.md §4.2); self is total minus
	// the children's totals. Suspension intersects each invocation's
	// [enter, exit] with the Options.Suspend timeline
	// (08-ui-backend-requirements.md R7), split self/total the same way.
	Node struct {
		MethodIdx        int
		DurationMs       int64
		SelfDurationMs   int64
		SuspensionMs     int64
		SelfSuspensionMs int64
		Executions       int64
		SelfExecutions   int64
		Params           []Param
		Children         []*Node
	}

	// Param is one parameter of a node (§2.5.3): an aggregated mini-tree, not
	// a flat value list — a node can hold thousands of SQL texts
	// (08-ui-backend-requirements.md R11). Groups is ordered by durationMs
	// descending, with the ::other bucket, when present, last.
	Param struct {
		ParamIdx int
		Groups   []ParamGroup
	}

	// ParamGroup is one aggregated value group (§2.5.3). Value carries the
	// first-seen full text, the literal "::other" for the overflow bucket, or
	// the "<stream>:<seq>:<offset>" reference text when Unresolved — an
	// unresolvable value is marked, never dropped silently. Params nests
	// binds under their SQL.
	ParamGroup struct {
		Value      string
		DurationMs int64
		Executions int64
		Params     []Param
		Unresolved bool
	}

	// Options resolve the blob's dictionary ids and big-parameter references
	// and carry the suspension timeline the tree attributes from.
	Options struct {
		// Dict resolves a dictionary id to its word. A missing word renders as
		// the "#<id>" placeholder, matching the /calls list path.
		Dict func(id int) (string, bool)
		// BigValue resolves a (stream, rolling_seq, offset) reference into the
		// sql / xml value streams (01-write-contract.md §4.4). false marks the
		// value unresolved. Nil treats every reference as unresolved.
		BigValue func(stream string, seq int, offset int64) (string, bool)
		// Suspend is the pod-restart's global stop-the-world timeline
		// (08-ui-backend-requirements.md R7): on the hot tier the replica's
		// suspend.wal mirror, on the cold tier the suspend/v1 snapshot. Both
		// are agent wall-clock Unix ms — the same clock as the trace timer
		// epoch, so intervals intersect without translation. Build sorts and
		// merges the pauses itself; empty means zero suspension everywhere.
		Suspend []SuspendInterval
		// MaxParamGroups caps the value groups per container — a node's
		// top-level params jointly, or one group's nested params (02 §2.5.3).
		// 0 means the contract default of 256 (the Java Hotspot.MAX_PARAMS).
		MaxParamGroups int
	}

	// SuspendInterval is one stop-the-world pause of the suspension timeline.
	SuspendInterval struct {
		TimeMs     int64
		DurationMs int64
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

// Build decodes the blob into the merged call tree (08-ui-backend-requirements.md
// R5): sibling invocations of one method under a parent fold into one node,
// so the node count is bounded by distinct call paths, not by invocations — a
// million-iteration loop is one node, not a million. recordIndex is the PK
// component locating the root ENTER within the blob's first chunk
// (01-write-contract.md §4.5).
func Build(blob []byte, recordIndex int, opts Options) (*Tree, error) {
	maxGroups := opts.MaxParamGroups
	if maxGroups <= 0 {
		maxGroups = DefaultMaxParamGroups
	}
	b := &builder{
		opts:       opts,
		tree:       &Tree{Methods: []string{}, Params: []string{}},
		methodIdx:  map[string]int{},
		paramIdx:   map[string]int{},
		childIdx:   map[*Node]map[int]*Node{},
		containers: map[*Node]*container{},
		maxGroups:  maxGroups,
		pauses:     normalizeSuspend(opts.Suspend),
	}
	if err := walkCall(blob, recordIndex, b.visit); err != nil {
		return nil, err
	}
	if b.tree.Root != nil {
		totalExecutions(b.tree.Root)
		b.materializeParams(b.tree.Root)
	}
	return b.tree, nil
}

// normalizeSuspend sorts the timeline and merges overlapping or touching
// pauses, so overlapMs can binary-search and never double-counts. The input
// is not mutated; agent pauses are already sorted and disjoint in practice,
// making this a cheap copy.
//
// SuspendInterval.TimeMs is the pause END (the agent timestamps a delay after
// detecting it; the reference SuspendLog builds start = date − delay), so a
// pause spans [TimeMs − DurationMs, TimeMs]. The merge keys off that start and
// keeps the (end, duration) representation (№4).
func normalizeSuspend(pauses []SuspendInterval) []SuspendInterval {
	if len(pauses) == 0 {
		return nil
	}
	sorted := append([]SuspendInterval(nil), pauses...)
	start := func(p SuspendInterval) int64 { return p.TimeMs - p.DurationMs }
	sort.Slice(sorted, func(i, j int) bool { return start(sorted[i]) < start(sorted[j]) })
	out := sorted[:1]
	for _, p := range sorted[1:] {
		last := &out[len(out)-1]
		if start(p) <= last.TimeMs { // p starts before or as the previous pause ends
			if p.TimeMs > last.TimeMs { // extend the merged end, keeping the earlier start
				last.DurationMs = p.TimeMs - start(*last)
				last.TimeMs = p.TimeMs
			}
			continue
		}
		out = append(out, p)
	}
	return out
}

// overlapMs sums the intersection of [fromMs, toMs) with the normalized
// timeline — the same arithmetic the seal pass applies to whole calls
// (01 §5.1 step 4), here per invocation. Each pause spans
// [TimeMs − DurationMs, TimeMs] (TimeMs is the pause end).
func overlapMs(pauses []SuspendInterval, fromMs, toMs int64) int64 {
	// A normalized timeline is sorted by start; find the first pause whose end
	// is past fromMs (earlier pauses cannot intersect [fromMs, toMs)).
	first := sort.Search(len(pauses), func(i int) bool {
		return pauses[i].TimeMs > fromMs
	})
	total := int64(0)
	for _, p := range pauses[first:] {
		start := p.TimeMs - p.DurationMs
		if start >= toMs {
			break
		}
		lo, hi := fromMs, toMs
		if start > lo {
			lo = start
		}
		if p.TimeMs < hi {
			hi = p.TimeMs
		}
		if hi > lo {
			total += hi - lo
		}
	}
	return total
}

// totalExecutions rolls invocation counts up the merged tree:
// executions = selfExecutions + Σ children.executions (02 §2.5.3), the
// old UI's M_EXECUTIONS + M_CHILD_EXECUTIONS displayed as "calls".
func totalExecutions(n *Node) int64 {
	n.Executions = n.SelfExecutions
	for _, child := range n.Children {
		n.Executions += totalExecutions(child)
	}
	return n.Executions
}

// builder folds the call's events into the merged tree. Each stack frame is
// one live invocation: it keeps the absolute enter time — the wire carries no
// durations, only the enter/exit pair (01 §4.2) — and the wall-clock this
// invocation's children consumed, so the exit adds both the invocation's
// total and its self time to the merged node in one pass. childIdx finds the
// merged node for a method under a parent in O(1); it persists across sibling
// invocations, which is what makes them fold into one node.
type builder struct {
	opts       Options
	tree       *Tree
	methodIdx  map[string]int
	paramIdx   map[string]int
	childIdx   map[*Node]map[int]*Node
	containers map[*Node]*container
	maxGroups  int
	pauses     []SuspendInterval
	stack      []frame
}

type frame struct {
	node           *Node
	enterMs        int64
	childrenMs     int64
	childrenSuspMs int64
	tags           []invocationTag
}

// invocationTag is one tag event of a live invocation, held until the exit
// knows the invocation's duration to attribute (02 §2.5.3).
type invocationTag struct {
	paramIdx   int
	value      string
	unresolved bool
	isSQL      bool
	isBinds    bool
}

func (b *builder) visit(ev event, atMs int64) {
	switch ev.kind {
	case pipe.EventEnterRecord:
		methodIdx := b.internMethod(ev.tagId)
		var node *Node
		if b.tree.Root == nil {
			node = &Node{MethodIdx: methodIdx}
			b.tree.Root = node
		} else {
			parent := b.stack[len(b.stack)-1].node
			byMethod := b.childIdx[parent]
			if byMethod == nil {
				byMethod = map[int]*Node{}
				b.childIdx[parent] = byMethod
			}
			node = byMethod[methodIdx]
			if node == nil {
				node = &Node{MethodIdx: methodIdx}
				byMethod[methodIdx] = node
				parent.Children = append(parent.Children, node)
			}
		}
		node.SelfExecutions++
		b.stack = append(b.stack, frame{node: node, enterMs: atMs})
	case pipe.EventExitRecord:
		top := b.stack[len(b.stack)-1]
		duration := atMs - top.enterMs
		suspension := overlapMs(b.pauses, top.enterMs, atMs)
		top.node.DurationMs += duration
		top.node.SelfDurationMs += duration - top.childrenMs
		top.node.SuspensionMs += suspension
		top.node.SelfSuspensionMs += suspension - top.childrenSuspMs
		b.foldTags(top.node, top.tags, duration)
		b.stack = b.stack[:len(b.stack)-1]
		if len(b.stack) > 0 {
			parent := &b.stack[len(b.stack)-1]
			parent.childrenMs += duration
			parent.childrenSuspMs += suspension
		}
	case pipe.EventTagRecord:
		top := &b.stack[len(b.stack)-1]
		idx := b.internParam(ev.tagId)
		tag := invocationTag{paramIdx: idx}
		if isBigParam(ev.paramType) {
			ref := ev.bigRef()
			value, ok := "", false
			if b.opts.BigValue != nil {
				value, ok = b.opts.BigValue(ref.Stream, ref.Seq, ref.Offset)
			}
			if !ok {
				// Explicit, not silent: the group carries the reference text
				// and its Unresolved flag (02 §2.5.3).
				tag.unresolved = true
				value = ref.String()
			}
			tag.value = value
			// The deduplicated big-value stream carries SQL by construction
			// (01 §4.4): these groups key by the normalised signature, and
			// binds of the same invocation nest under them.
			tag.isSQL = int(ev.paramType) == pipe.ParamBigDedup
		} else {
			tag.value = ev.value
		}
		tag.isBinds = b.tree.Params[idx] == "binds"
		top.tags = append(top.tags, tag)
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
		// A corrupt blob can encode a length/ref above MaxInt64; the int64 cast
		// would wrap negative and defeat every downstream bounds check (a
		// negative make() panics). Reject it here so no caller sees a negative.
		if v > math.MaxInt64 {
			return 0, errors.Errorf("varint at chunk offset %d overflows int64", pos)
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
				// Each rune is 2 bytes in the chunk, so a valid count cannot
				// exceed the remaining bytes / 2. Checking against that budget
				// (rather than pos+2*runes) also avoids overflowing the multiply
				// before make() sees the count.
				if runes > int64(len(chunk)-pos)/2 {
					return pos, errors.New("torn tag string value")
				}
				// The agent writes each char as one UTF-16 code unit; decode the
				// run as a whole so surrogate pairs reassemble into full runes
				// (mirror PipeReader.ReadVarString, not a signed per-char cast).
				units := make([]uint16, runes)
				for i := range units {
					units[i] = binary.BigEndian.Uint16(chunk[pos+2*i:])
				}
				pos += 2 * int(runes)
				ev.value = string(utf16.Decode(units))
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
