package calltree

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleTree() *Tree {
	return &Tree{
		Methods: []string{"com.example.Service.handle", "com.example.Service.query"},
		Params:  []string{"request.id", "sql", "binds"},
		Root: &Node{
			MethodIdx: 0,
			Params: []Param{{
				ParamIdx: 0,
				Groups:   []ParamGroup{{Value: "req-1", DurationMs: 1247, Executions: 1}},
			}},
			Children: []*Node{
				{
					MethodIdx:        1,
					DurationMs:       40,
					SelfDurationMs:   40,
					SuspensionMs:     3,
					SelfSuspensionMs: 3,
					Executions:       12,
					SelfExecutions:   12,
					Params: []Param{{
						ParamIdx: 1,
						Groups: []ParamGroup{
							{
								Value: "SELECT 1", DurationMs: 25, Executions: 10,
								Params: []Param{{
									ParamIdx: 2,
									Groups:   []ParamGroup{{Value: "42", DurationMs: 25, Executions: 10}},
								}},
							},
							{Value: "sql:2:17", DurationMs: 10, Executions: 1, Unresolved: true},
							{Value: OtherGroupValue, DurationMs: 5, Executions: 1},
						},
					}},
				},
			},
			DurationMs:       1247,
			SelfDurationMs:   1207,
			SuspensionMs:     5,
			SelfSuspensionMs: 2,
			Executions:       1,
			SelfExecutions:   1,
		},
	}
}

func TestMsgpackRoundtrip(t *testing.T) {
	encoded := Encode(sampleTree())

	tree, version, err := Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, int64(Version), version)
	assert.Equal(t, sampleTree(), tree)
}

func TestMsgpackWideValues(t *testing.T) {
	// Values that leave the fix* ranges: 300 children (array16), a >31-char
	// method (str8), a large duration (int32) and a big string value.
	tree := &Tree{
		Methods: []string{strings.Repeat("m", 300)},
		Params:  []string{"sql"},
		Root:    &Node{DurationMs: 5_000_000_000},
	}
	for i := 0; i < 300; i++ {
		tree.Root.Children = append(tree.Root.Children, &Node{
			DurationMs: 1,
			Executions: int64(i),
			Params: []Param{{
				ParamIdx: 0,
				Groups:   []ParamGroup{{Value: strings.Repeat("v", 70_000), DurationMs: 1, Executions: 1}},
			}},
		})
	}

	decoded, _, err := Decode(Encode(tree))
	require.NoError(t, err)
	assert.Equal(t, tree, decoded)
}

// TestDecodeSkipsUnknownFields pins the §2.5.1 forward-compatibility rule an
// additive server change relies on: a decoder built for the v1 tables must
// ignore int keys it does not know, at every record level.
func TestDecodeSkipsUnknownFields(t *testing.T) {
	var e encoder
	e.putMapHeader(5)
	e.putInt(treeFieldV)
	e.putInt(Version)
	e.putInt(treeFieldMethods)
	e.putStrings([]string{"m"})
	e.putInt(treeFieldParams)
	e.putStrings(nil)
	e.putInt(treeFieldRoot)
	// A node with two unknown additive fields (9: string, 10: nested map) —
	// the next free numbers a future server may claim (02 §2.5.3).
	e.putMapHeader(5)
	e.putInt(nodeFieldMethodIdx)
	e.putInt(0)
	e.putInt(nodeFieldSelfExecutions)
	e.putInt(3)
	e.putInt(nodeFieldDurationMs)
	e.putInt(7)
	e.putInt(9)
	e.putString("future cpuMs rationale")
	e.putInt(10)
	e.putMapHeader(1)
	e.putInt(0)
	e.putInt(12345)
	// An unknown envelope field (9: array of ints) after the root.
	e.putInt(9)
	e.putArrayHeader(2)
	e.putInt(1)
	e.putInt(-200)

	tree, version, err := Decode(e.buf)
	require.NoError(t, err)
	assert.Equal(t, int64(1), version)
	assert.Equal(t, int64(7), tree.Root.DurationMs)
	assert.Equal(t, int64(3), tree.Root.SelfExecutions)
	assert.Equal(t, []string{"m"}, tree.Methods)
}

// FuzzDecode pins the decoder's failure mode on corrupted payloads: an error,
// never a panic or an unbounded allocation. Valid inputs must round-trip —
// what decodes must re-encode to an envelope that decodes to the same tree.
func FuzzDecode(f *testing.F) {
	f.Add(Encode(sampleTree()))
	f.Add([]byte{})
	f.Add([]byte{0x81, 0x00, 0x01})       // envelope without a root
	f.Add([]byte{0xdd, 0xff, 0xff, 0xff}) // truncated array32 header
	corrupted := Encode(sampleTree())
	corrupted[len(corrupted)/2] ^= 0xff
	f.Add(corrupted)

	f.Fuzz(func(t *testing.T, data []byte) {
		tree, _, err := Decode(data)
		if err != nil {
			return
		}
		again, _, err := Decode(Encode(tree))
		require.NoError(t, err, "a decoded tree must re-encode cleanly")
		require.Equal(t, tree, again)
	})
}
