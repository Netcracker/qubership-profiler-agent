//go:build integration

package integration

import (
	"context"
	"os"
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
	genCfg := generator.SimpleConfig(1, 1, 1)
	// GenerateCalls replays a captured pod dump (ui5min.bin) that the
	// project never commits (WORKFLOW.md §6); loadPodData would otherwise
	// log.Fatal on the missing file, so skip instead.
	if _, err := os.Stat(genCfg.PathToPodFile); os.IsNotExist(err) {
		suite.T().Skip("missing captured fixture " + genCfg.PathToPodFile +
			" (real pod dumps are never committed; see WORKFLOW.md §6)")
	}

	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
	suite.timestamp = time.Date(2024, 4, 3, 0, 0, 0, 0, time.UTC)

	suite.pg = helpers.CreatePgContainer(suite.ctx, suite.timestamp)

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
