//go:build integration

package config

import (
	"os"
	"testing"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/stretchr/testify/assert"
)

func TestServerCfg_ExportConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	os.Setenv(constants.NcCloudNamespace, "ns")
	os.Setenv(constants.NcMicroServiceName, "example")
	os.Setenv(constants.ConfigServerAddress, "http://localhost:2183")
	args := []string{testDir, "invalid", constants.NcDiagMode, constants.NcDiagDumpInterval}

	testMockMap["/etc/secret/username"] = "vvv"
	testMockMap["/etc/secret/password"] = "asd"
	testMockMap["m2mToken"] = "tasd"
	testMockMap["aclToken"] = ""

	t.Run("docker", func(t *testing.T) {
		serverCfg := &ServerCfg{}
		err := serverCfg.Prepare(args)
		assert.Nil(t, err)
		err = serverCfg.InitConfig(testCtx)
		assert.Nil(t, err)

		err = serverCfg.ExportConfig(testCtx)
		assert.Nil(t, err)

		data, exists, err := serverCfg.readFullFile(testCtx, serverCfg.propFile(constants.NcDiagMode))
		assert.Nil(t, err)
		assert.True(t, exists)
		assert.Equal(t, "val103", string(data))

		data, exists, err = serverCfg.readFullFile(testCtx, serverCfg.propFile(constants.NcDiagDumpInterval))
		assert.Nil(t, err)
		assert.True(t, exists)
		assert.Equal(t, "103", string(data))

		data, exists, err = serverCfg.readFullFile(testCtx, serverCfg.propFile("invalid"))
		assert.False(t, exists)
	})
}
