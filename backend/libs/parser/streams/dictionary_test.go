package streams

import (
	"context"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

func TestReadDictionary_TestService(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	testDictionaryFile := filepath.Join(ResourceDir, "test-service", "test-service.dictionary.protocol")
	expectedDictionaryLog := filepath.Join(ResourceDir, "test-service", "test-service.dictionary.expected.txt")

	t.Run("dictionary", func(t *testing.T) {
		c := testChunk(t, model.StreamDictionary, testDictionaryFile)

		dict, res, err := ReadDictionary(ctx, c)
		assert.Nil(t, err)
		assert.Equal(t, readTestFile(t, expectedDictionaryLog), stripLines(res))
		require.Equal(t, 942, len(dict.List))
		assert.Equal(t, "call.idle", dict.List[2].Word)
		assert.Equal(t, "exception", dict.List[3].Word)
		assert.Equal(t, "brave.parent_id", dict.List[147].Word)
	})
}

func TestReadDictionary_5minService(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	testDictionaryFile := filepath.Join(ResourceDir, "u5min", "u5min-service.dictionary.protocol")
	expectedDictionaryLog := filepath.Join(ResourceDir, "u5min", "u5min-service.dictionary.expected.txt")

	t.Run("dictionary", func(t *testing.T) {
		c := testChunk(t, model.StreamDictionary, testDictionaryFile)

		dict, res, err := ReadDictionary(ctx, c)
		assert.Nil(t, err)
		//fmt.Print(res)
		assert.Equal(t, readTestFile(t, expectedDictionaryLog), stripLines(res))
		require.Equal(t, 976, len(dict.List))
		assert.Equal(t, "call.idle", dict.List[2].Word)
		assert.Equal(t, "exception", dict.List[3].Word)
		assert.Equal(t, "brave.parent_id", dict.List[147].Word)
	})
}
