package pipe

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
)

func (suite *IntegrationTestSuite) TestParamsStream() {
	t := suite.T()
	//ctx := log.SetLevel(context.Background(), log.DEBUG)

	c := suite.testChunk(model.StreamParams)
	assert.Equal(t, model.StreamParams, c.StreamType)
	assert.Equal(t, 0, c.SequenceId)

	expected := suite.readTestFile("ui5min.expected.params.list.txt", "\n")
	pipe := ParamsPipeReader(suite.ctx, OpenDataAsReader(c.Data, false))
	suite.compareParamsList(expected, pipe)

	expected = suite.readTestFile("ui5min.expected.params.txt", Separator)
	pipe = ParamsPipeReader(suite.ctx, OpenDataAsReader(c.Data, false))
	suite.compareParamsPipe(expected, pipe)

}

func (suite *IntegrationTestSuite) compareParamsPipe(expected []string, pipe <-chan ParamItem) {
	//suite.writeTestFile("ui5min.expected.params.txt", func(f *os.File) {
	//    for s := range pipe {
	//        fmt.Fprintf(f, "%d%s%s%s%d%s%s%s%d%s", s.Id, Separator, s.Name, Separator,
	//            s.Order, Separator, s.Signature, Separator, s.pos, Separator)
	//    }
	//    fmt.Fprintf(f, "EOF")
	//})
	i := 0
	for s := range pipe {
		require.True(suite.T(), len(expected) > i+4) // need at least five elements in expected

		val, err := strconv.Atoi(expected[i])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.Id))

		assert.Equal(suite.T(), expected[i+1], s.Name)

		val, err = strconv.Atoi(expected[i+2])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.Order))

		assert.Equal(suite.T(), expected[i+3], s.Signature)

		val, err = strconv.Atoi(expected[i+4])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.pos))

		i += 5
	}
	assert.Equal(suite.T(), len(expected)/5, i/5)
}

// only names
func (suite *IntegrationTestSuite) compareParamsList(expected []string, pipe <-chan ParamItem) {
	i := 0
	for s := range pipe {
		require.True(suite.T(), len(expected) > i) // need at least one element in expected
		require.Equal(suite.T(), expected[i], s.Name)
		i++
	}
	assert.Equal(suite.T(), len(expected), i)
}
