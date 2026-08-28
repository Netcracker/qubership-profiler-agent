package pipe

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"time"
)

func (suite *IntegrationTestSuite) TestSuspendStream() {
	t := suite.T()
	//ctx := log.SetLevel(context.Background(), log.DEBUG)

	c := suite.testChunk(model.StreamSuspend)
	assert.Equal(t, model.StreamSuspend, c.StreamType)
	assert.Equal(t, 0, c.SequenceId)

	expected := suite.readTestFile("ui5min.expected.suspend.txt", Separator)
	pipe := SuspendPipeReader(suite.ctx, OpenDataAsReader(c.Data, false))
	suite.compareSuspendPipe(expected, pipe)

}

func (suite *IntegrationTestSuite) compareSuspendPipe(expected []string, pipe <-chan SuspendItem) {
	//suite.writeTestFile("ui5min.expected.suspend.txt", func(f *os.File) {
	//    for s := range pipe {
	//        fmt.Fprintf(f, "%d%s%d%s%d%s%s%s%d%s", s.id, Separator, s.delta, Separator,
	//            s.Amount, Separator, s.Time.Format(time.RFC3339), Separator, s.pos, Separator)
	//    }
	//    fmt.Fprintf(f, "EOF")
	//})
	i := 0
	for s := range pipe {
		require.True(suite.T(), len(expected) > i+4) // need at least five elements in expected

		val, err := strconv.Atoi(expected[i])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.id))

		val, err = strconv.Atoi(expected[i+1])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, s.delta)

		val, err = strconv.Atoi(expected[i+2])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.Amount))

		ts, err := time.Parse(time.RFC3339, expected[i+3])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), ts.UTC(), s.Time.Truncate(time.Second).UTC())

		val, err = strconv.Atoi(expected[i+4])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.pos))

		i += 5
	}
	assert.Equal(suite.T(), len(expected)/5, i/5)
}
