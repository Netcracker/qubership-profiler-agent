package wire

import "bytes"

// DictionaryStream encodes dictionary words in the PROTOCOL_VERSION_V2 wire
// format: one phrase — a fixed-int byte length followed by var-strings — with
// word ids implied by arrival order (06-wire-protocol-server.md §3,
// backend/libs/parser/pipe/dictionary.go).
func DictionaryStream(words []string) []byte {
	body := &bytes.Buffer{}
	for _, w := range words {
		putVarString(body, w)
	}
	return phrase(body.Bytes())
}

// ParamDef is one parameter-metadata record of the params stream.
type ParamDef struct {
	Name      string
	IsIndex   bool
	IsList    bool
	Order     int
	Signature string
}

// ParamsStream encodes the params stream: one phrase opening with the format
// version byte, then the records (backend/libs/parser/pipe/params.go).
func ParamsStream(params []ParamDef) []byte {
	body := &bytes.Buffer{}
	body.WriteByte(1) // format version
	for _, p := range params {
		putVarString(body, p.Name)
		putBool(body, p.IsIndex)
		putBool(body, p.IsList)
		putVarInt(body, uint64(p.Order))
		putVarString(body, p.Signature)
	}
	return phrase(body.Bytes())
}

// SuspendEvent is one stop-the-world pause. DeltaMs is the delta to the pause
// END from the previous event's end (from the stream base for the first), and
// AmountMs is the duration. The agent timestamps a delay after detecting it, so
// the wire carries the pause end, not its start; a pause spans
// [end − AmountMs, end] (backend/libs/parser/pipe/suspend.go, №4).
type SuspendEvent struct {
	DeltaMs  int
	AmountMs int
}

// SuspendStream encodes the suspend stream: one phrase opening with the 8-byte
// absolute base time, then (delta, amount) varint pairs
// (backend/libs/parser/pipe/suspend.go).
func SuspendStream(baseMs int64, events []SuspendEvent) []byte {
	body := &bytes.Buffer{}
	putFixedLong(body, uint64(baseMs))
	for _, e := range events {
		putVarInt(body, uint64(e.DeltaMs))
		putVarInt(body, uint64(e.AmountMs))
	}
	return phrase(body.Bytes())
}

// phrase wraps a body in the agent's phrase framing: a fixed-int byte length,
// then the body (PhraseOutputStream.writeDataIntoOutputStream).
func phrase(body []byte) []byte {
	buf := &bytes.Buffer{}
	putFixedInt(buf, uint32(len(body)))
	buf.Write(body)
	return buf.Bytes()
}

func putBool(buf *bytes.Buffer, v bool) {
	if v {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
}
