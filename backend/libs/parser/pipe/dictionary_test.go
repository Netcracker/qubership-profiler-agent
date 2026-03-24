package pipe

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
)

func (suite *IntegrationTestSuite) TestDictionaryStream() {
	t := suite.T()
	//ctx := log.SetLevel(context.Background(), log.DEBUG)

	c := suite.testChunk(model.StreamDictionary)
	assert.Equal(t, model.StreamDictionary, c.StreamType)
	assert.Equal(t, 0, c.SequenceId)

	expected := suite.readTestFile("ui5min.expected.dict.list.txt", "\n")
	pipe := DictionaryPipeReader(suite.ctx, OpenDataAsReader(c.Data, false), 1000)
	suite.compareDictionaryList(expected, pipe)

	expected = suite.readTestFile("ui5min.expected.dict.txt", Separator)
	pipe = DictionaryPipeReader(suite.ctx, OpenDataAsReader(c.Data, false), 1000)
	//pipe := DictionaryV2PipeReader(suite.ctx, OpenDataAsReader(c.Data))
	suite.compareDictionaryPipe(expected, pipe)

}

func (suite *IntegrationTestSuite) compareDictionaryPipe(expected []string, pipe <-chan DictionaryItem) {
	//suite.writeTestFile("ui5min.expected.dict.txt", func(f *os.File) {
	//    for s := range pipe {
	//        fmt.Fprintf(f, "%d%s%s%s", s.id, Separator, s.value, Separator)
	//    }
	//    fmt.Fprintf(f, "EOF")
	//})
	i := 0
	for s := range pipe {
		require.True(suite.T(), len(expected) > i+1) // need more two elements in expected
		val, err := strconv.Atoi(expected[i])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.Id))
		assert.Equal(suite.T(), expected[i+1], s.Value)
		i += 2
	}
	assert.Equal(suite.T(), len(expected)/2, i/2)
}

// only names
func (suite *IntegrationTestSuite) compareDictionaryList(expected []string, pipe <-chan DictionaryItem) {
	i := 0
	for s := range pipe {
		require.True(suite.T(), len(expected) > i) // need at least one element in expected
		require.Equal(suite.T(), expected[i], s.Value)
		i++
	}
	assert.Equal(suite.T(), len(expected), i)
}
