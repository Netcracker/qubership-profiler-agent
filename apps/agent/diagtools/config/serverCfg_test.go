package config

import (
	"os"
	"testing"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"

	"github.com/stretchr/testify/assert"
)

func TestServerCfg_InitConfig(t *testing.T) {

	os.Setenv(constants.NcCloudNamespace, "ns")
	os.Setenv(constants.NcMicroServiceName, "svc")

	args := []string{testDir, "invalid", constants.NcDiagMode, constants.NcDiagDumpInterval}
	os.Setenv(constants.ConfigServerAddress, "http://localhost:2183")

	t.Run("default", func(t *testing.T) {
		serverCfg := &ServerCfg{}
		err := serverCfg.Prepare(args)
		assert.Nil(t, err)
		assert.Equal(t, testDir, serverCfg.TargetFolder)
		assert.Equal(t, []string{"invalid", "NC_DIAGNOSTIC_MODE", "DIAGNOSTIC_DUMP_INTERVAL"}, serverCfg.Properties)

		err = serverCfg.InitConfig(testCtx)
		assert.Nil(t, err)

		assert.Equal(t, "http://localhost:2183", serverCfg.ConfigServerUrl)
		assert.Equal(t, "http://identity-provider:8080", serverCfg.IdpUrl)
		assert.Equal(t, "ns", serverCfg.Namespace)
		assert.Equal(t, "svc", serverCfg.ServiceName)
	})
}
