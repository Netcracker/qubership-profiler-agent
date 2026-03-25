//go:build integration

package integration

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/generator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"time"
)

func (suite *PGTestSuite) TestInsertCalls() {
	t := suite.T()

	for i := 0; i < len(suite.gen.Calls); i++ {
		call := generator.Convert(*suite.gen.Calls[i])
		err := suite.pg.Client.InsertCall(suite.ctx, call)
		require.NoError(t, err)
	}

	calls, err := suite.pg.Client.GetCallsTimeBetween(suite.ctx, "ns-0", suite.timestamp.Add(-5*time.Minute))
	require.NoError(t, err)
	assert.Equal(t, 0, len(calls))

	calls, err = suite.pg.Client.GetCallsTimeBetween(suite.ctx, "ns-0", suite.timestamp)
	require.NoError(t, err)
	assert.Equal(t, 10, len(calls))

	calls, err = suite.pg.Client.GetCallsTimeBetween(suite.ctx, "ns-0", suite.timestamp.Add(-5*time.Minute))
	require.NoError(t, err)
	assert.Equal(t, 0, len(calls))
}
