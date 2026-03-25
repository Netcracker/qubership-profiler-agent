package generator

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPrepareSuite(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	opts := createTestOptions()
	data, err := LoadData(ctx, opts)
	assert.Nil(t, err)

	t.Run("suite", func(t *testing.T) {
		s, err := PrepareSuite(ctx, opts, data)
		assert.Nil(t, err)

		assert.Equal(t, 10, s.PodCount)
		assert.Contains(t, s.RandomTdDump().Filename, ".td.txt")
		assert.Contains(t, s.RandomTopDump().Filename, ".top.txt")
		assert.Equal(t, "profiler:esc-ui-service:esc-ui-service-84967fdd77-pwhw4_1690201584049", s.RandomTcpDump().Name())

		p1 := s.Pod(1, "scenario")
		assert.Equal(t, "ns_", p1.Namespace)

		p2 := s.Pod(2, "scenario")
		assert.Equal(t, "ns_", p2.Namespace)

		p3 := s.Pod(2, "scenario2")
		assert.Equal(t, "ns_", p3.Namespace)

		p11 := s.Pod(1, "scenario") // should be cached
		assert.Equal(t, "ns_", p11.Namespace)
		assert.Equal(t, p1.PodName, p11.PodName)
	})
}
