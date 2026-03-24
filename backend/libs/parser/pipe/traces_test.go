package pipe

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"time"
)

func (suite *IntegrationTestSuite) TestTracesStream() {
	t := suite.T()
	//ctx := log.SetLevel(context.Background(), log.DEBUG)

	c := suite.testChunk(model.StreamTrace)
	assert.Equal(t, model.StreamTrace, c.StreamType)
	assert.Equal(t, 0, c.SequenceId)

	expected := suite.readTestFile("ui5min.expected.traces.txt", Separator)
	pipe := TracesPipeReader(suite.ctx, OpenDataAsReader(c.Data, true))
	suite.compareTracesPipe(expected, pipe)

}

func (suite *IntegrationTestSuite) compareTracesPipe(expected []string, pipe <-chan TraceItem) {
	//suite.writeTestFile("ui5min.expected.traces.txt", func(f *os.File) {
	//   for s := range pipe {
	//       fmt.Fprintf(f, "%d%s%d%s%s%s%d%s%s%s", s.Id, Separator, s.Offset, Separator,
	//           s.Time.Format(time.RFC3339), Separator, s.bytes, Separator, s.debug, Separator)
	//   }
	//   fmt.Fprintf(f, "EOF")
	//})
	i := 0
	for s := range pipe {
		require.True(suite.T(), len(expected) > i+4) // need at least five elements in expected

		val, err := strconv.Atoi(expected[i])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.Id))

		val, err = strconv.Atoi(expected[i+1])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), int64(val), s.Offset)

		ts, err := time.Parse(time.RFC3339, expected[i+2])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), ts.UTC(), s.Time.Truncate(time.Second).UTC())

		val, err = strconv.Atoi(expected[i+3])
		require.NoError(suite.T(), err)
		assert.Equal(suite.T(), val, int(s.bytes))

		assert.Equal(suite.T(), expected[i+4], s.debug)

		//assert.True(suite.T(), len(s.Data) > 0)
		assert.Equal(suite.T(), s.bytes, len(s.Data), "trace #%d", i/5)

		i += 5
	}
	assert.Equal(suite.T(), len(expected)/5, i/5)
}
