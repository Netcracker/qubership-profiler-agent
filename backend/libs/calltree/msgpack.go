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

// Field numbers of a Node (§2.5.3).
const (
	nodeFieldMethodIdx  = 0
	nodeFieldEnterMsRel = 1
	nodeFieldDurationMs = 2
	nodeFieldParams     = 3
	nodeFieldChildren   = 4
)

// Field numbers of a Param (§2.5.3).
const (
	paramFieldParamIdx   = 0
	paramFieldValues     = 1
	paramFieldUnresolved = 2
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
	fields := 3
	if len(n.Params) > 0 {
		fields++
	}
	if len(n.Children) > 0 {
		fields++
	}
	e.putMapHeader(fields)
	e.putInt(nodeFieldMethodIdx)
	e.putInt(int64(n.MethodIdx))
	e.putInt(nodeFieldEnterMsRel)
	e.putInt(n.EnterMsRel)
	e.putInt(nodeFieldDurationMs)
	e.putInt(n.DurationMs)
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
	fields := 2
	if len(p.Unresolved) > 0 {
		fields++
	}
	e.putMapHeader(fields)
	e.putInt(paramFieldParamIdx)
	e.putInt(int64(p.ParamIdx))
	e.putInt(paramFieldValues)
	e.putStrings(p.Values)
	if len(p.Unresolved) > 0 {
		e.putInt(paramFieldUnresolved)
		e.putArrayHeader(len(p.Unresolved))
		for _, i := range p.Unresolved {
			e.putInt(int64(i))
		}
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
	tree := &Tree{Methods: []string{}, Params: []string{}}
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
	return tree, version, nil
}

type decoder struct {
	data []byte
	pos  int
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
		case nodeFieldEnterMsRel:
			if node.EnterMsRel, err = d.int(); err != nil {
				return nil, err
			}
		case nodeFieldDurationMs:
			if node.DurationMs, err = d.int(); err != nil {
				return nil, err
			}
		case nodeFieldParams:
			cnt, err := d.arrayHeader()
			if err != nil {
				return nil, err
			}
			node.Params = make([]Param, 0, cnt)
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
			node.Children = make([]*Node, 0, cnt)
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
		case paramFieldValues:
			if p.Values, err = d.strings(); err != nil {
				return Param{}, err
			}
		case paramFieldUnresolved:
			cnt, err := d.arrayHeader()
			if err != nil {
				return Param{}, err
			}
			p.Unresolved = make([]int, 0, cnt)
			for j := 0; j < cnt; j++ {
				v, err := d.int()
				if err != nil {
					return Param{}, err
				}
				p.Unresolved = append(p.Unresolved, int(v))
			}
		default:
			if err := d.skip(); err != nil {
				return Param{}, err
			}
		}
	}
	return p, nil
}

func (d *decoder) strings() ([]string, error) {
	n, err := d.arrayHeader()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
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
		return int64(v), err
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
