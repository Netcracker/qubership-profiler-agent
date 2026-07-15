package calltree

// MessagePack codec for the /tree response (02-read-contract.md §2.5.1-§2.5.4):
// every record is a Map<int, value> whose field numbers are pinned by the
// contract's tag tables, additions append at the next free number, and
// decoders skip unknown keys. The codec is hand-written on purpose — the
// contract trades a schema toolchain for ~100 LOC per consumer language, and
// Decode below doubles as the reference decoder a consumer would port.

import (
	"encoding/binary"
	"math"

	"github.com/pkg/errors"
)

// Version is the current format version carried in the envelope's v field.
const Version = 1

// Field numbers of the Tree envelope (§2.5.3).
const (
	treeFieldV       = 0
	treeFieldMethods = 1
	treeFieldParams  = 2
	treeFieldRoot    = 3
)

// Field numbers of a Node (§2.5.3, merged v1). The original raw-tree v1
// (enterMsRel at 1, durationMs at 2, params at 3, children at 4) shipped to
// no consumer, so v1 was redefined in place rather than bumped — see the
// "v1 redefined" note in the contract.
const (
	nodeFieldMethodIdx        = 0
	nodeFieldDurationMs       = 1
	nodeFieldSelfDurationMs   = 2
	nodeFieldSuspensionMs     = 3
	nodeFieldSelfSuspensionMs = 4
	nodeFieldExecutions       = 5
	nodeFieldSelfExecutions   = 6
	nodeFieldParams           = 7
	nodeFieldChildren         = 8
)

// Field numbers of a Param (§2.5.3). 1 (the pre-R11 flat values list) and
// 2 (its unresolved index list) are reserved — see the contract's
// reserved-number registry.
const (
	paramFieldParamIdx = 0
	paramFieldGroups   = 3
)

// Field numbers of a ParamGroup (§2.5.3).
const (
	groupFieldValue      = 0
	groupFieldDurationMs = 1
	groupFieldExecutions = 2
	groupFieldParams     = 3
	groupFieldUnresolved = 4
)

// Encode renders the tree as the §2.5.2 MessagePack envelope.
func Encode(t *Tree) []byte {
	var e encoder
	e.putMapHeader(4)
	e.putInt(treeFieldV)
	e.putInt(Version)
	e.putInt(treeFieldMethods)
	e.putStrings(t.Methods)
	e.putInt(treeFieldParams)
	e.putStrings(t.Params)
	e.putInt(treeFieldRoot)
	e.putNode(t.Root)
	return e.buf
}

type encoder struct{ buf []byte }

func (e *encoder) putNode(n *Node) {
	fields := 7
	if len(n.Params) > 0 {
		fields++
	}
	if len(n.Children) > 0 {
		fields++
	}
	e.putMapHeader(fields)
	e.putInt(nodeFieldMethodIdx)
	e.putInt(int64(n.MethodIdx))
	e.putInt(nodeFieldDurationMs)
	e.putInt(n.DurationMs)
	e.putInt(nodeFieldSelfDurationMs)
	e.putInt(n.SelfDurationMs)
	e.putInt(nodeFieldSuspensionMs)
	e.putInt(n.SuspensionMs)
	e.putInt(nodeFieldSelfSuspensionMs)
	e.putInt(n.SelfSuspensionMs)
	e.putInt(nodeFieldExecutions)
	e.putInt(n.Executions)
	e.putInt(nodeFieldSelfExecutions)
	e.putInt(n.SelfExecutions)
	if len(n.Params) > 0 {
		e.putInt(nodeFieldParams)
		e.putArrayHeader(len(n.Params))
		for i := range n.Params {
			e.putParam(&n.Params[i])
		}
	}
	if len(n.Children) > 0 {
		e.putInt(nodeFieldChildren)
		e.putArrayHeader(len(n.Children))
		for _, child := range n.Children {
			e.putNode(child)
		}
	}
}

func (e *encoder) putParam(p *Param) {
	e.putMapHeader(2)
	e.putInt(paramFieldParamIdx)
	e.putInt(int64(p.ParamIdx))
	e.putInt(paramFieldGroups)
	e.putArrayHeader(len(p.Groups))
	for i := range p.Groups {
		e.putGroup(&p.Groups[i])
	}
}

func (e *encoder) putGroup(g *ParamGroup) {
	fields := 3
	if len(g.Params) > 0 {
		fields++
	}
	if g.Unresolved {
		fields++
	}
	e.putMapHeader(fields)
	e.putInt(groupFieldValue)
	e.putString(g.Value)
	e.putInt(groupFieldDurationMs)
	e.putInt(g.DurationMs)
	e.putInt(groupFieldExecutions)
	e.putInt(g.Executions)
	if len(g.Params) > 0 {
		e.putInt(groupFieldParams)
		e.putArrayHeader(len(g.Params))
		for i := range g.Params {
			e.putParam(&g.Params[i])
		}
	}
	if g.Unresolved {
		e.putInt(groupFieldUnresolved)
		e.buf = append(e.buf, 0xc3) // msgpack true
	}
}

func (e *encoder) putStrings(ss []string) {
	e.putArrayHeader(len(ss))
	for _, s := range ss {
		e.putString(s)
	}
}

func (e *encoder) putMapHeader(n int) {
	switch {
	case n < 16:
		e.buf = append(e.buf, 0x80|byte(n))
	case n < 1<<16:
		e.buf = append(e.buf, 0xde, byte(n>>8), byte(n))
	default:
		e.buf = binary.BigEndian.AppendUint32(append(e.buf, 0xdf), uint32(n))
	}
}

func (e *encoder) putArrayHeader(n int) {
	switch {
	case n < 16:
		e.buf = append(e.buf, 0x90|byte(n))
	case n < 1<<16:
		e.buf = append(e.buf, 0xdc, byte(n>>8), byte(n))
	default:
		e.buf = binary.BigEndian.AppendUint32(append(e.buf, 0xdd), uint32(n))
	}
}

func (e *encoder) putInt(v int64) {
	switch {
	case v >= 0 && v < 128:
		e.buf = append(e.buf, byte(v))
	case v < 0 && v >= -32:
		e.buf = append(e.buf, byte(v))
	case v >= math.MinInt8 && v <= math.MaxInt8:
		e.buf = append(e.buf, 0xd0, byte(v))
	case v >= math.MinInt16 && v <= math.MaxInt16:
		e.buf = append(e.buf, 0xd1, byte(v>>8), byte(v))
	case v >= math.MinInt32 && v <= math.MaxInt32:
		e.buf = binary.BigEndian.AppendUint32(append(e.buf, 0xd2), uint32(v))
	default:
		e.buf = binary.BigEndian.AppendUint64(append(e.buf, 0xd3), uint64(v))
	}
}

func (e *encoder) putString(s string) {
	n := len(s)
	switch {
	case n < 32:
		e.buf = append(e.buf, 0xa0|byte(n))
	case n < 1<<8:
		e.buf = append(e.buf, 0xd9, byte(n))
	case n < 1<<16:
		e.buf = append(e.buf, 0xda, byte(n>>8), byte(n))
	default:
		e.buf = binary.BigEndian.AppendUint32(append(e.buf, 0xdb), uint32(n))
	}
	e.buf = append(e.buf, s...)
}

// Decode parses a §2.5.2 envelope back into a Tree, returning the envelope's
// v alongside. It is the reference decoder of §2.5.1: unknown int keys are
// skipped, so an old client keeps working when the server appends fields.
func Decode(data []byte) (*Tree, int64, error) {
	d := &decoder{data: data}
	tree := &Tree{}
	version := int64(0)
	n, err := d.mapHeader()
	if err != nil {
		return nil, 0, err
	}
	for i := 0; i < n; i++ {
		key, err := d.int()
		if err != nil {
			return nil, 0, err
		}
		switch key {
		case treeFieldV:
			if version, err = d.int(); err != nil {
				return nil, 0, err
			}
		case treeFieldMethods:
			if tree.Methods, err = d.strings(); err != nil {
				return nil, 0, err
			}
		case treeFieldParams:
			if tree.Params, err = d.strings(); err != nil {
				return nil, 0, err
			}
		case treeFieldRoot:
			if tree.Root, err = d.node(); err != nil {
				return nil, 0, err
			}
		default:
			if err := d.skip(); err != nil {
				return nil, 0, err
			}
		}
	}
	if tree.Root == nil {
		return nil, 0, errors.New("envelope has no root node")
	}
	// One envelope per frame: trailing bytes mean a truncated stream, a framing
	// bug, or an adversarial append, none of which should decode as success.
	if d.pos != len(data) {
		return nil, 0, errors.Errorf("%d trailing bytes after the envelope", len(data)-d.pos)
	}
	return tree, version, nil
}

type decoder struct {
	data []byte
	pos  int
}

// prealloc caps a header-declared count before it becomes an allocation: a
// hostile array32 header can claim 2^32 elements while the payload holds
// none. append grows the slice to the real size; the cap only bounds the
// upfront reservation.
func prealloc(n int) int {
	const max = 1024
	if n > max {
		return max
	}
	return n
}

func (d *decoder) node() (*Node, error) {
	n, err := d.mapHeader()
	if err != nil {
		return nil, err
	}
	node := &Node{}
	for i := 0; i < n; i++ {
		key, err := d.int()
		if err != nil {
			return nil, err
		}
		switch key {
		case nodeFieldMethodIdx:
			v, err := d.int()
			if err != nil {
				return nil, err
			}
			node.MethodIdx = int(v)
		case nodeFieldDurationMs:
			if node.DurationMs, err = d.int(); err != nil {
				return nil, err
			}
		case nodeFieldSelfDurationMs:
			if node.SelfDurationMs, err = d.int(); err != nil {
				return nil, err
			}
		case nodeFieldSuspensionMs:
			if node.SuspensionMs, err = d.int(); err != nil {
				return nil, err
			}
		case nodeFieldSelfSuspensionMs:
			if node.SelfSuspensionMs, err = d.int(); err != nil {
				return nil, err
			}
		case nodeFieldExecutions:
			if node.Executions, err = d.int(); err != nil {
				return nil, err
			}
		case nodeFieldSelfExecutions:
			if node.SelfExecutions, err = d.int(); err != nil {
				return nil, err
			}
		case nodeFieldParams:
			cnt, err := d.arrayHeader()
			if err != nil {
				return nil, err
			}
			if cnt > 0 {
				// An empty optional array stays nil: Encode omits the field,
				// so nil is the canonical form a round-trip preserves.
				node.Params = make([]Param, 0, prealloc(cnt))
			}
			for j := 0; j < cnt; j++ {
				p, err := d.param()
				if err != nil {
					return nil, err
				}
				node.Params = append(node.Params, p)
			}
		case nodeFieldChildren:
			cnt, err := d.arrayHeader()
			if err != nil {
				return nil, err
			}
			if cnt > 0 {
				node.Children = make([]*Node, 0, prealloc(cnt))
			}
			for j := 0; j < cnt; j++ {
				child, err := d.node()
				if err != nil {
					return nil, err
				}
				node.Children = append(node.Children, child)
			}
		default:
			if err := d.skip(); err != nil {
				return nil, err
			}
		}
	}
	return node, nil
}

func (d *decoder) param() (Param, error) {
	n, err := d.mapHeader()
	if err != nil {
		return Param{}, err
	}
	p := Param{}
	for i := 0; i < n; i++ {
		key, err := d.int()
		if err != nil {
			return Param{}, err
		}
		switch key {
		case paramFieldParamIdx:
			v, err := d.int()
			if err != nil {
				return Param{}, err
			}
			p.ParamIdx = int(v)
		case paramFieldGroups:
			cnt, err := d.arrayHeader()
			if err != nil {
				return Param{}, err
			}
			if cnt > 0 {
				p.Groups = make([]ParamGroup, 0, prealloc(cnt))
			}
			for j := 0; j < cnt; j++ {
				g, err := d.group()
				if err != nil {
					return Param{}, err
				}
				p.Groups = append(p.Groups, g)
			}
		default:
			if err := d.skip(); err != nil {
				return Param{}, err
			}
		}
	}
	return p, nil
}

func (d *decoder) group() (ParamGroup, error) {
	n, err := d.mapHeader()
	if err != nil {
		return ParamGroup{}, err
	}
	g := ParamGroup{}
	for i := 0; i < n; i++ {
		key, err := d.int()
		if err != nil {
			return ParamGroup{}, err
		}
		switch key {
		case groupFieldValue:
			if g.Value, err = d.string(); err != nil {
				return ParamGroup{}, err
			}
		case groupFieldDurationMs:
			if g.DurationMs, err = d.int(); err != nil {
				return ParamGroup{}, err
			}
		case groupFieldExecutions:
			if g.Executions, err = d.int(); err != nil {
				return ParamGroup{}, err
			}
		case groupFieldParams:
			cnt, err := d.arrayHeader()
			if err != nil {
				return ParamGroup{}, err
			}
			if cnt > 0 {
				g.Params = make([]Param, 0, prealloc(cnt))
			}
			for j := 0; j < cnt; j++ {
				p, err := d.param()
				if err != nil {
					return ParamGroup{}, err
				}
				g.Params = append(g.Params, p)
			}
		case groupFieldUnresolved:
			if g.Unresolved, err = d.bool(); err != nil {
				return ParamGroup{}, err
			}
		default:
			if err := d.skip(); err != nil {
				return ParamGroup{}, err
			}
		}
	}
	return g, nil
}

func (d *decoder) bool() (bool, error) {
	b, err := d.byte()
	if err != nil {
		return false, err
	}
	switch b {
	case 0xc2:
		return false, nil
	case 0xc3:
		return true, nil
	default:
		return false, errors.Errorf("expected a bool, got 0x%02x", b)
	}
}

func (d *decoder) strings() ([]string, error) {
	n, err := d.arrayHeader()
	if err != nil {
		return nil, err
	}
	// nil for an empty array: Encode writes the same empty array for nil, so
	// nil is the canonical form a round-trip preserves.
	var out []string
	if n > 0 {
		out = make([]string, 0, prealloc(n))
	}
	for i := 0; i < n; i++ {
		s, err := d.string()
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (d *decoder) byte() (byte, error) {
	if d.pos >= len(d.data) {
		return 0, errors.New("truncated msgpack value")
	}
	b := d.data[d.pos]
	d.pos++
	return b, nil
}

func (d *decoder) take(n int) ([]byte, error) {
	if d.pos+n > len(d.data) {
		return nil, errors.New("truncated msgpack value")
	}
	b := d.data[d.pos : d.pos+n]
	d.pos += n
	return b, nil
}

func (d *decoder) uintN(n int) (uint64, error) {
	b, err := d.take(n)
	if err != nil {
		return 0, err
	}
	v := uint64(0)
	for _, x := range b {
		v = v<<8 | uint64(x)
	}
	return v, nil
}

func (d *decoder) mapHeader() (int, error) {
	b, err := d.byte()
	if err != nil {
		return 0, err
	}
	switch {
	case b&0xf0 == 0x80:
		return int(b & 0x0f), nil
	case b == 0xde:
		v, err := d.uintN(2)
		return int(v), err
	case b == 0xdf:
		v, err := d.uintN(4)
		return int(v), err
	default:
		return 0, errors.Errorf("expected a map, got 0x%02x", b)
	}
}

func (d *decoder) arrayHeader() (int, error) {
	b, err := d.byte()
	if err != nil {
		return 0, err
	}
	switch {
	case b&0xf0 == 0x90:
		return int(b & 0x0f), nil
	case b == 0xdc:
		v, err := d.uintN(2)
		return int(v), err
	case b == 0xdd:
		v, err := d.uintN(4)
		return int(v), err
	default:
		return 0, errors.Errorf("expected an array, got 0x%02x", b)
	}
}

func (d *decoder) int() (int64, error) {
	b, err := d.byte()
	if err != nil {
		return 0, err
	}
	switch {
	case b < 0x80: // positive fixint
		return int64(b), nil
	case b >= 0xe0: // negative fixint
		return int64(int8(b)), nil
	}
	switch b {
	case 0xcc:
		v, err := d.uintN(1)
		return int64(v), err
	case 0xcd:
		v, err := d.uintN(2)
		return int64(v), err
	case 0xce:
		v, err := d.uintN(4)
		return int64(v), err
	case 0xcf:
		v, err := d.uintN(8)
		if err != nil {
			return 0, err
		}
		// The tree carries only non-negative metrics; a uint64 past MaxInt64
		// would wrap negative on the cast and re-emit a negative count. Reject
		// it so a corrupted frame fails instead of round-tripping garbage.
		if v > math.MaxInt64 {
			return 0, errors.Errorf("uint64 %d overflows int64", v)
		}
		return int64(v), nil
	case 0xd0:
		v, err := d.uintN(1)
		return int64(int8(v)), err
	case 0xd1:
		v, err := d.uintN(2)
		return int64(int16(v)), err
	case 0xd2:
		v, err := d.uintN(4)
		return int64(int32(v)), err
	case 0xd3:
		v, err := d.uintN(8)
		return int64(v), err
	default:
		return 0, errors.Errorf("expected an int, got 0x%02x", b)
	}
}

func (d *decoder) string() (string, error) {
	b, err := d.byte()
	if err != nil {
		return "", err
	}
	var n int
	switch {
	case b&0xe0 == 0xa0:
		n = int(b & 0x1f)
	case b == 0xd9:
		v, err := d.uintN(1)
		if err != nil {
			return "", err
		}
		n = int(v)
	case b == 0xda:
		v, err := d.uintN(2)
		if err != nil {
			return "", err
		}
		n = int(v)
	case b == 0xdb:
		v, err := d.uintN(4)
		if err != nil {
			return "", err
		}
		n = int(v)
	default:
		return "", errors.Errorf("expected a string, got 0x%02x", b)
	}
	s, err := d.take(n)
	return string(s), err
}

// skip drops one value of any type — the §2.5.1 forward-compatibility rule.
func (d *decoder) skip() error {
	b, err := d.byte()
	if err != nil {
		return err
	}
	switch {
	case b < 0x80 || b >= 0xe0: // fixint
		return nil
	case b&0xe0 == 0xa0: // fixstr
		_, err := d.take(int(b & 0x1f))
		return err
	case b&0xf0 == 0x80: // fixmap
		return d.skipPairs(int(b & 0x0f))
	case b&0xf0 == 0x90: // fixarray
		return d.skipMany(int(b & 0x0f))
	}
	switch b {
	case 0xc0, 0xc2, 0xc3: // nil, false, true
		return nil
	case 0xcc, 0xd0:
		_, err := d.take(1)
		return err
	case 0xcd, 0xd1:
		_, err := d.take(2)
		return err
	case 0xce, 0xd2, 0xca:
		_, err := d.take(4)
		return err
	case 0xcf, 0xd3, 0xcb:
		_, err := d.take(8)
		return err
	case 0xd9:
		n, err := d.uintN(1)
		if err != nil {
			return err
		}
		_, err = d.take(int(n))
		return err
	case 0xda:
		n, err := d.uintN(2)
		if err != nil {
			return err
		}
		_, err = d.take(int(n))
		return err
	case 0xdb:
		n, err := d.uintN(4)
		if err != nil {
			return err
		}
		_, err = d.take(int(n))
		return err
	case 0xdc:
		n, err := d.uintN(2)
		if err != nil {
			return err
		}
		return d.skipMany(int(n))
	case 0xdd:
		n, err := d.uintN(4)
		if err != nil {
			return err
		}
		return d.skipMany(int(n))
	case 0xde:
		n, err := d.uintN(2)
		if err != nil {
			return err
		}
		return d.skipPairs(int(n))
	case 0xdf:
		n, err := d.uintN(4)
		if err != nil {
			return err
		}
		return d.skipPairs(int(n))
	default:
		return errors.Errorf("cannot skip msgpack type 0x%02x", b)
	}
}

func (d *decoder) skipMany(n int) error {
	for i := 0; i < n; i++ {
		if err := d.skip(); err != nil {
			return err
		}
	}
	return nil
}

func (d *decoder) skipPairs(n int) error {
	return d.skipMany(2 * n)
}
