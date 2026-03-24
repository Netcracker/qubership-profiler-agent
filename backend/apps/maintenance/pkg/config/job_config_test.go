package config

import (
	"context"
	"path"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/stretchr/testify/assert"
)

func TestParseJobConfig(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	testDir := "../../tests/resources/config"
	expectedDefaultJobConfig := JobConfig{
		TempTableCreation: 2,
		TempTableRemoval:  2,
		S3FileRemoval: S3RemoveJobConfig{
			Calls: CallsS3RemoveJobConfig{
				Map: map[model.DurationRange]TimeHours{
					*model.Durations.GetByName("0ms"):   24 * 14,
					*model.Durations.GetByName("1ms"):   24 * 14,
					*model.Durations.GetByName("10ms"):  24 * 14,
					*model.Durations.GetByName("100ms"): 24 * 14,
					*model.Durations.GetByName("1s"):    24 * 14,
					*model.Durations.GetByName("5s"):    24 * 14,
					*model.Durations.GetByName("30s"):   24 * 14,
					*model.Durations.GetByName("90s"):   24 * 14,
				},
			},
			Dumps: DumpsS3RemoveJobConfig{
				Map: map[model.DumpType]TimeHours{
					model.DumpTypeTd:         24 * 14,
					model.DumpTypeTop:        24 * 14,
					model.DumpTypeGc:         24 * 14,
					model.DumpTypeAlloc:      24 * 14,
					model.DumpTypeGoroutine:  24 * 14,
					model.DumpTypeHeap:       24 * 14,
					model.DumpTypeProfile:    24 * 14,
					model.DumpTypeThreadInfo: 24 * 14,
				},
			},
			Heaps: 24 * 14,
		},
		MetadataRemoval: 24 * 14,
	}

	t.Run("job config", func(t *testing.T) {
		t.Run("not defined", func(t *testing.T) {
			s := log.CaptureAsString(func() {
				jobConfig, err := ParseConfigFromFile(ctx, "")
				assert.NoError(t, err)
				assert.Equal(t, expectedDefaultJobConfig, *jobConfig)
			})
			assert.Contains(t, s, "No job config file specified, default one will be used")
		})
		t.Run("wrong calls duration range", func(t *testing.T) {
			_, err := ParseConfigFromFile(ctx, path.Join(testDir, "wrong_calls_dr.yaml"))
			assert.ErrorContains(t, err, "found unsupported duration range in configuration: wrong_dr")
		})
		t.Run("wrong dump type", func(t *testing.T) {
			_, err := ParseConfigFromFile(ctx, path.Join(testDir, "wrong_dump_type.yaml"))
			assert.ErrorContains(t, err, "found unsupported dump type in configuration: wrong_dt")
		})
		t.Run("valid", func(t *testing.T) {
			expectedConfig := expectedDefaultJobConfig
			expectedConfig.TempTableCreation = 1
			expectedConfig.S3FileRemoval.Calls.Map[*model.Durations.GetByName("0ms")] = 2
			expectedConfig.S3FileRemoval.Dumps.Map[model.DumpTypeGc] = 3
			expectedConfig.S3FileRemoval.Heaps = 4

			jobConfig, err := ParseConfigFromFile(ctx, path.Join(testDir, "valid_config.yaml"))
			assert.NoError(t, err)
			assert.Equal(t, expectedConfig, *jobConfig)
		})
	})
}
