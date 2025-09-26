package config

import (
	"os"
	"testing"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"

	"github.com/stretchr/testify/assert"
)

func TestConsulCfg_InitConfig(t *testing.T) {

	err := os.Setenv(constants.NcCloudNamespace, "ns")
	assert.NoError(t, err)
	err = os.Setenv(constants.NcMicroServiceName, "svc")
	assert.NoError(t, err)

	args := []string{testDir, "invalid", constants.NcDiagMode, constants.NcDiagDumpInterval}
	err = os.Setenv(constants.ConsulEnabled, "true")
	assert.NoError(t, err)
	err = os.Setenv(constants.ConsulAddress, "http://localhost:2082")
	assert.NoError(t, err)

	t.Run("default", func(t *testing.T) {
		consulCfg := &ConsulCfg{}
		err := consulCfg.Prepare(args)
		assert.Nil(t, err)
		assert.Equal(t, testDir, consulCfg.TargetFolder)
		assert.Equal(t, []string{"invalid", "NC_DIAGNOSTIC_MODE", "DIAGNOSTIC_DUMP_INTERVAL"}, consulCfg.Properties)

		err = consulCfg.InitConfig(testCtx)
		assert.Nil(t, err)

		assert.Equal(t, "http://localhost:2082", consulCfg.ConsulUrl)
		assert.Equal(t, "http://identity-provider:8080", consulCfg.IdpUrl)
		assert.Equal(t, "ns", consulCfg.Namespace)
		assert.Equal(t, "svc", consulCfg.ServiceName)
	})
}
