package streams

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

func TestReadParams_TestService(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	testParamsFile := filepath.Join(ResourceDir, "test-service", "test-service.params.protocol")
	expectedParamsLog := filepath.Join(ResourceDir, "test-service", "test-service.params.expected.txt")

	t.Run("params", func(t *testing.T) {
		c := testChunk(t, model.StreamParams, testParamsFile)

		params, res, err := ReadParams(ctx, c)
		assert.Nil(t, err)
		assert.Equal(t, readTestFile(t, expectedParamsLog), stripLines(res))
		assert.Equal(t, 122, len(params.List))
	})
}

func TestReadParams_5minService(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	testParamsFile := filepath.Join(ResourceDir, "u5min", "u5min-service.params.protocol")
	expectedParamsLog := filepath.Join(ResourceDir, "u5min", "u5min-service.params.expected.txt")

	testOtherFile := filepath.Join(ResourceDir, "test-service", "test-service.params.protocol")

	t.Run("params", func(t *testing.T) {
		c := testChunk(t, model.StreamParams, testParamsFile)

		params, res, err := ReadParams(ctx, c)
		assert.Nil(t, err)
		assert.Equal(t, readTestFile(t, expectedParamsLog), stripLines(res))
		assert.Equal(t, 122, len(params.List))

		// params from other services should be the same:
		//  same version ESC => same plugins => same params
		c = testChunk(t, model.StreamParams, testOtherFile)
		params, res, err = ReadParams(ctx, c)
		assert.Nil(t, err)
		assert.Equal(t, readTestFile(t, expectedParamsLog), stripLines(res))
		assert.Equal(t, 122, len(params.List))
	})
}
