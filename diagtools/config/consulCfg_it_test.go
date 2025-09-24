//go:build integration

package config

import (
	"os"
	"testing"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/stretchr/testify/assert"
)

// integration test (see docker compose)

func TestConsulCfg_ExportConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	os.Setenv(constants.NcCloudNamespace, "ns")
	os.Setenv(constants.NcMicroServiceName, "svc")
	os.Setenv(constants.ConsulEnabled, "true")
	os.Setenv(constants.ConsulAddress, "http://localhost:2082")
	args := []string{testDir, "invalid", constants.NcDiagMode, constants.NcDiagDumpInterval}

	testMockMap["/etc/secret/username"] = "vvv"
	testMockMap["/etc/secret/password"] = "asd"
	testMockMap["m2mToken"] = "tasd"
	testMockMap["aclToken"] = ""

	t.Run("docker", func(t *testing.T) {
		consulCfg := &ConsulCfg{}
		err := consulCfg.Prepare(args)
		assert.Nil(t, err)
		err = consulCfg.InitConfig(testCtx)
		assert.Nil(t, err)
		err = consulCfg.prepareClient(testCtx, "")
		assert.Nil(t, err)

		// use default service
		err = consulCfg.setProperty(testCtx, "", constants.NcDiagMode, "val102")
		assert.Nil(t, err)
		// use "global" service
		err = consulCfg.setProperty(testCtx, constants.ServerAppPath, constants.NcDiagDumpInterval, "102")
		assert.Nil(t, err)

		val, err := consulCfg.configFromConsul(constants.NcDiagMode)
		assert.Nil(t, err)
		assert.NotNil(t, val)
		assert.Equal(t, "val102", string(val))
		val, err = consulCfg.configFromConsul(constants.NcDiagDumpInterval)
		assert.Nil(t, err)
		assert.NotNil(t, val)
		assert.Equal(t, "102", string(val))

		err = consulCfg.ExportConfig(testCtx)
		assert.Nil(t, err)

		data, exists, err := consulCfg.readFullFile(testCtx, consulCfg.propFile(constants.NcDiagMode))
		assert.Nil(t, err)
		assert.True(t, exists)
		assert.Equal(t, "val102", string(data))

		data, exists, err = consulCfg.readFullFile(testCtx, consulCfg.propFile(constants.NcDiagDumpInterval))
		assert.Nil(t, err)
		assert.True(t, exists)
		assert.Equal(t, "102", string(data))

		data, exists, err = consulCfg.readFullFile(testCtx, consulCfg.propFile("invalid"))
		assert.False(t, exists)
	})
}
