package config

import (
	"os"
	"testing"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"

	"github.com/stretchr/testify/assert"
)

func TestConsulCfg_InitConfig(t *testing.T) {

	os.Setenv(constants.NcCloudNamespace, "ns")
	os.Setenv(constants.NcMicroServiceName, "svc")

	args := []string{testDir, "invalid", constants.NcDiagMode, constants.NcDiagDumpInterval}
	os.Setenv(constants.ConsulEnabled, "true")
	os.Setenv(constants.ConsulAddress, "http://localhost:2082")

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
