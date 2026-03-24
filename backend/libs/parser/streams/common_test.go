package streams

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/stretchr/testify/require"
)

const (
	ResourceDir = "../../tests/resources/streams/"
)

var (
	uuid0 = common.ToUuid([16]byte{})
)

func testChunk(t *testing.T, stream model.StreamType, fileName string) *model.Chunk {
	c, err := readChunk(uuid0, stream, 0, fileName)
	require.Nil(t, err)
	return c
}

func readChunk(uuid common.Uuid, stream model.StreamType, seqId int, fileName string) (*model.Chunk, error) {
	data, err := readStringFile(fileName)
	if err != nil {
		return nil, err
	}
	c := model.NewChunk(uuid, stream, seqId, 0, 0)
	c.InitString(data)
	return c, nil
}

func stripLines(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n") // win
	s = strings.ReplaceAll(s, "\r", "\n")   // lin
	return strings.TrimSpace(s)
}

func readTestFile(t *testing.T, fileName string) string {
	s, err := readStringFile(fileName)
	require.Nil(t, err)
	return stripLines(s)
}

func readStringFile(fileName string) (string, error) {
	path, err := filepath.Abs(fileName)
	if err != nil {
		return "", err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
