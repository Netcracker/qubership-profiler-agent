package streams

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

func TestReadStringStreams_TestService(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	t.Run("sql", func(t *testing.T) {
		testSqlFile := filepath.Join(ResourceDir, "test-service", "test-service.sql.0.protocol")
		expectedSqlData := filepath.Join(ResourceDir, "test-service", "test-service.sql.expected.txt")

		c := testChunk(t, model.StreamSql, testSqlFile)
		res := ReadStringStream(ctx, model.StreamSql, c)
		assert.Equal(t, readTestFile(t, expectedSqlData), stripLines(res))
	})

	t.Run("xml", func(t *testing.T) {
		testXmlFile := filepath.Join(ResourceDir, "test-service", "test-service.xml.0.protocol")
		expectedXmlData := filepath.Join(ResourceDir, "test-service", "test-service.xml.expected.txt")

		c := testChunk(t, model.StreamXml, testXmlFile)
		res := ReadStringStream(ctx, model.StreamXml, c)
		assert.Equal(t, readTestFile(t, expectedXmlData), stripLines(res))
	})
}

func TestReadStringStreams_5minService(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	t.Run("sql", func(t *testing.T) {
		testSqlFile := filepath.Join(ResourceDir, "u5min", "u5min-service.sql.0.protocol")
		expectedSqlData := filepath.Join(ResourceDir, "u5min", "u5min-service.sql.expected.txt")

		c := testChunk(t, model.StreamSql, testSqlFile)
		res := ReadStringStream(ctx, model.StreamSql, c)
		assert.Equal(t, readTestFile(t, expectedSqlData), stripLines(res))
	})

	t.Run("xml", func(t *testing.T) {
		testXmlFile := filepath.Join(ResourceDir, "u5min", "u5min-service.xml.0.protocol")
		expectedXmlData := filepath.Join(ResourceDir, "u5min", "u5min-service.xml.expected.txt")

		c := testChunk(t, model.StreamXml, testXmlFile)
		res := ReadStringStream(ctx, model.StreamXml, c)
		//fmt.Print(res)
		assert.Equal(t, readTestFile(t, expectedXmlData), stripLines(res))
	})
}
