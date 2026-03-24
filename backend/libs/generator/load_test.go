package generator

import (
	"context"
	"fmt"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestLoadData(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	opts := createTestOptions()

	t.Run("valid", func(t *testing.T) {
		s, err := LoadData(ctx, opts)
		assert.Nil(t, err)
		assert.Nil(t, s.Validate())
		assert.Equal(t, 4, len(s.TopDumps))
		assert.Equal(t, 3, len(s.TdDumps))
		assert.Equal(t, 1, len(s.TcpDumps))

		opts.DataDir = filepath.Join(DataDir, "invalid")
		s, err = LoadData(ctx, opts) // use cache
		assert.Nil(t, err)
		assert.Nil(t, s.Validate())
		assert.Equal(t, 4, len(s.TopDumps))
		assert.Equal(t, 3, len(s.TdDumps))
		assert.Equal(t, 1, len(s.TcpDumps))
	})

	t.Run("invalid", func(t *testing.T) {
		opts.DataDir = ""
		s, err := LoadData(ctx, opts)
		assert.ErrorContains(t, err, "empty path for data")
		assert.Nil(t, s)

		ClearCache(ctx)

		opts.DataDir = filepath.Join(DataDir, "invalid")
		fmt.Print(opts)
		s, err = LoadData(ctx, opts)
		//assert.ErrorContains(t, err, "The system cannot find the path specified") // win
		//assert.ErrorContains(t, err, "no such file or directory") // lin
		assert.ErrorContains(t, err, "dumps.td")
		assert.ErrorContains(t, s.Validate(), "no prepared tcp dumps")
		assert.Equal(t, 0, len(s.TopDumps))
		assert.Equal(t, 0, len(s.TdDumps))
		assert.Equal(t, 0, len(s.TcpDumps))

	})
}
