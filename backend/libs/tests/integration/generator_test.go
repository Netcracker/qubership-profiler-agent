package integration

import (
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/generator"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

func TestGenerator_GenerateCalls(t *testing.T) {
	ts := time.Date(2024, 4, 3, 0, 0, 0, 0, time.UTC)

	ctx := log.SetLevel(log.Context("utest"), log.DEBUG)
	t.Run("run", func(t *testing.T) {
		cfg := generator.SimpleConfig(1, 1, 1)
		g := generator.NewGenerator(cfg, ts)
		g.GenerateCalls(ctx)

		assert.Equal(t, 10, len(g.Calls))
		for _, c := range g.Calls {
			assert.Equal(t, "ns-0", c.Namespace)
			assert.Equal(t, "svc-0", c.ServiceName)
			assert.Equal(t, "pod-0", c.PodName)
		}
	})
}

func TestGenerator_GenerateDumps(t *testing.T) {
	ts := time.Date(2024, 4, 3, 0, 0, 0, 0, time.UTC)

	ctx := log.SetLevel(log.Context("utest"), log.DEBUG)
	t.Run("run", func(t *testing.T) {
		cfg := generator.SimpleConfig(1, 1, 1)
		g := generator.NewGenerator(cfg, ts)
		g.GenerateDumps(ctx)
		assert.Equal(t, 7, len(g.Dumps))
	})
}
