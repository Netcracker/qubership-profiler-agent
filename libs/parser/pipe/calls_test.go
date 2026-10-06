package pipe

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
)

func (suite *IntegrationTestSuite) TestCallsStream() {
	t := suite.T()
	//ctx := log.SetLevel(context.Background(), log.DEBUG)

	c := suite.testChunk(model.StreamCalls)
	assert.Equal(t, model.StreamCalls, c.StreamType)
	assert.Equal(t, 0, c.SequenceId)

	expected := suite.readTestFile("ui5min.expected.calls.txt", Separator)
	pipe := CallsPipeReader(suite.ctx, OpenDataAsReader(c.Data, false))
	suite.compareCallsPipe(expected, pipe)

}

func (suite *IntegrationTestSuite) compareCallsPipe(expected []string, pipe <-chan CallItem) {
	//suite.writeTestFile("ui5min.expected.calls.txt", func(f *os.File) {
	//    for s := range pipe {
	//        fmt.Fprintf(f, "%d%s%d%s%s%s", s.id, Separator,
	//            s.pos, Separator,
	//            s.Call.Csv(), Separator,
	//        )
	//    }
	//    fmt.Fprintf(f, "EOF")
	//})
	i := 0
	for s := range pipe {
		require.True(suite.T(), len(expected) > i+2) // need at least five elements in expected

		val, err := strconv.Atoi(expected[i])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.id))

		val, err = strconv.Atoi(expected[i+1])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.pos))

		assert.Equal(suite.T(), expected[i+2], s.Call.Csv())

		i += 3
	}
	assert.Equal(suite.T(), len(expected)/3, i/3)
}
