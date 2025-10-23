//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/generator"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/suite"
)

type PGTestSuite struct {
	suite.Suite
	ctx       context.Context
	timestamp time.Time
	pg        *helpers.PostgresContainer
	gen       *generator.Generator
}

func (suite *PGTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
	suite.timestamp = time.Date(2024, 4, 3, 0, 0, 0, 0, time.UTC)

	suite.pg = helpers.CreatePgContainer(suite.ctx, suite.timestamp)

	genCfg := generator.SimpleConfig(1, 1, 1)
	suite.gen = generator.NewGenerator(genCfg, suite.timestamp)
	suite.gen.GenerateCalls(suite.ctx)
	suite.gen.GenerateDumps(suite.ctx)
}

func (suite *PGTestSuite) TearDownSuite() {
	if err := suite.pg.Terminate(suite.ctx); err != nil {
		log.Error(suite.ctx, err, "error terminating pg container")
		suite.FailNow("tear down")
	}
}

func TestPGTestSuite(t *testing.T) {
	suite.Run(t, new(PGTestSuite))
}
