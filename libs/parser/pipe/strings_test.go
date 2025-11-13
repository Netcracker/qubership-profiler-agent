package pipe

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func (suite *IntegrationTestSuite) TestStringStreams() {
	t := suite.T()
	//ctx := log.SetLevel(context.Background(), log.DEBUG)

	t.Run("sql", func(t *testing.T) {
		c := suite.testChunk(model.StreamSql)
		assert.Equal(t, model.StreamSql, c.StreamType)
		assert.Equal(t, 0, c.SequenceId)
		expected := suite.readTestFile("ui5min.expected.sql.txt", Separator)
		pipe := StringPipeReader(suite.ctx, OpenDataAsReader(c.Data, false))
		suite.compareStringPipe("sql", expected, pipe)
	})

	t.Run("xml", func(t *testing.T) {
		c := suite.testChunk(model.StreamXml)
		assert.Equal(t, model.StreamXml, c.StreamType)
		assert.Equal(t, 0, c.SequenceId)
		expected := suite.readTestFile("ui5min.expected.xml.txt", Separator)
		pipe := StringPipeReader(suite.ctx, OpenDataAsReader(c.Data, false))
		suite.compareStringPipe("xml", expected, pipe)
	})

}

func (suite *IntegrationTestSuite) compareStringPipe(stream string, expected []string, pipe <-chan StringItem) {
	//filename := fmt.Sprintf("ui5min.expected.%s.txt", stream)
	//suite.writeTestFile(filename, func(f *os.File) {
	//    for s := range pipe {
	//        fmt.Fprintf(f, "%d%s%d%s%s%s", s.Id, Separator, s.pos, Separator, s.Value, Separator)
	//    }
	//    fmt.Fprintf(f, "EOF")
	//})
	i := 0
	for s := range pipe {
		require.True(suite.T(), len(expected) > i+2) // need at least three elements in expected

		val, err := strconv.Atoi(expected[i])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.Id))

		val, err = strconv.Atoi(expected[i+1])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.pos))

		assert.Equal(suite.T(), expected[i+2], s.Value)

		i += 3
	}
	assert.Equal(suite.T(), len(expected)/3, i/3)
}
