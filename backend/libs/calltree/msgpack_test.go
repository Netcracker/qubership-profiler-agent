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
		Params:  []string{"request.id", "sql"},
		Root: &Node{
			MethodIdx: 0,
			Params:    []Param{{ParamIdx: 0, Values: []string{"req-1"}}},
			Children: []*Node{
				{
					MethodIdx:  1,
					EnterMsRel: 2,
					DurationMs: 40,
					Params: []Param{{
						ParamIdx:   1,
						Values:     []string{"SELECT 1", "sql:2:17"},
						Unresolved: []int{1},
					}},
				},
			},
			DurationMs: 1247,
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
			EnterMsRel: int64(i),
			DurationMs: 1,
			Params:     []Param{{ParamIdx: 0, Values: []string{strings.Repeat("v", 70_000)}}},
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
	// A node with two unknown additive fields (5: string, 6: nested map).
	e.putMapHeader(5)
	e.putInt(nodeFieldMethodIdx)
	e.putInt(0)
	e.putInt(nodeFieldEnterMsRel)
	e.putInt(0)
	e.putInt(nodeFieldDurationMs)
	e.putInt(7)
	e.putInt(5)
	e.putString("future cpuMs rationale")
	e.putInt(6)
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
	assert.Equal(t, []string{"m"}, tree.Methods)
}
