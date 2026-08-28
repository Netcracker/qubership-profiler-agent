//go:build integration

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"

	"github.com/stretchr/testify/assert"
)

// integration test (see docker compose)

func TestZkCfg_ExportConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	os.Setenv(constants.NcCloudNamespace, "ns")
	os.Setenv(constants.NcMicroServiceName, "svc")
	os.Setenv(constants.ZkAddress, "localhost:2181") // see docker compose
	args := []string{testDir, "invalid", constants.NcDiagMode, constants.NcDiagDumpInterval}

	t.Run("docker", func(t *testing.T) {
		zkCfg := &ZkCfg{}
		err := zkCfg.Prepare(args)
		assert.Nil(t, err)
		assert.Equal(t, testDir, zkCfg.TargetFolder)
		assert.Equal(t, []string{"invalid", "NC_DIAGNOSTIC_MODE", "DIAGNOSTIC_DUMP_INTERVAL"}, zkCfg.Properties)

		uploadDataToZk(t, zkCfg)

		err = zkCfg.ExportConfig(testCtx)
		assert.Nil(t, err)
		assert.Equal(t, []string{"localhost:2181"}, zkCfg.ZkAddresses)
		assert.NotNil(t, zkCfg.ZkConnect)

		data, exists, err := zkCfg.readFullFile(testCtx, zkCfg.propFile(constants.NcDiagMode))
		assert.Nil(t, err)
		assert.True(t, exists)
		assert.Equal(t, "val101", string(data))

		data, exists, err = zkCfg.readFullFile(testCtx, zkCfg.propFile(constants.NcDiagDumpInterval))
		assert.Nil(t, err)
		assert.True(t, exists)
		assert.Equal(t, "101", string(data))

		data, exists, err = zkCfg.readFullFile(testCtx, zkCfg.propFile("invalid"))
		assert.False(t, exists)
	})
}

func TestZkCfg_FilterProperties(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	os.Setenv(constants.NcCloudNamespace, "ns")
	os.Setenv(constants.NcMicroServiceName, "svc")
	os.Setenv(constants.ZkAddress, "localhost:2181") // see docker compose
	args := []string{testDir, "invalid", constants.NcDiagMode, constants.NcDiagAgentService}

	t.Run("filter", func(t *testing.T) {
		zkCfg := &ZkCfg{}
		err := zkCfg.Prepare(args)
		assert.Nil(t, err)

		uploadDataToZk(t, zkCfg)

		err = zkCfg.ExportConfig(testCtx)
		assert.Nil(t, err)

		zkCfg.Properties = []string{"asd", "bsd", "cde"}
		err = zkCfg.FilterProperties(testCtx)
		assert.Nil(t, err)

		assert.Equal(t, "", os.Getenv(constants.NcDiagMode))
		assert.Equal(t, "", os.Getenv(constants.NcDiagAgentService))

		zkCfg.Properties = []string{"asd", "bsd", "cde", constants.NcDiagMode, constants.NcDiagAgentService}
		err = zkCfg.FilterProperties(testCtx)
		assert.Nil(t, err)

		assert.Equal(t, "val101", os.Getenv(constants.NcDiagMode))
		assert.Equal(t, "svc101", os.Getenv(constants.NcDiagAgentService))
	})
}

func TestZkCfg_MoveEscConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	os.Setenv(constants.NcCloudNamespace, "ns")
	os.Setenv(constants.NcMicroServiceName, "svc")
	os.Setenv(constants.ZkAddress, "localhost:2181") // see docker compose
	os.Setenv(constants.NcDiagnosticFolder, testDir)
	args := []string{testDir, "invalid", constants.NcDiagMode, constants.ZkCustomConfig}

	err := os.MkdirAll(filepath.Join(testDir, "config", "default"), 0755)
	assert.Nil(t, err)

	t.Run("filter", func(t *testing.T) {
		zkCfg := &ZkCfg{}
		err := zkCfg.Prepare(args)
		assert.Nil(t, err)

		uploadDataToZk(t, zkCfg)

		err = zkCfg.ExportConfig(testCtx)
		assert.Nil(t, err)

		err = zkCfg.CheckEscConfigFile(testCtx, constants.ZkCustomConfig)
		assert.Nil(t, err)

		data, exists, err := zkCfg.readFullFile(testCtx, filepath.Join(testDir, "config", constants.ZkConfigXmlFilePath))
		assert.Nil(t, err)
		assert.True(t, exists)
		assert.Equal(t, strings.TrimSpace(escConfig()), strings.TrimSpace(string(data)))
	})
}

func uploadDataToZk(t *testing.T, zkCfg *ZkCfg) {
	zkCfg.inConnect(testCtx, func() {
		// use default service
		zkCfg.setProperty(testCtx, "", constants.NcDiagMode, "val101")
		// use default service
		zkCfg.setProperty(testCtx, "", constants.NcDiagAgentService, "svc101")
		// use default service
		zkCfg.setProperty(testCtx, "", constants.ZkCustomConfig, escConfig())
		// use "global" service
		zkCfg.setProperty(testCtx, constants.ServerAppPath, constants.NcDiagDumpInterval, "101")

		val := zkCfg.configFromZK(testCtx, constants.NcDiagMode)
		assert.NotNil(t, val)
		assert.Equal(t, "val101", *val)
		val = zkCfg.configFromZK(testCtx, constants.NcDiagAgentService)
		assert.NotNil(t, val)
		assert.Equal(t, "svc101", *val)
		val = zkCfg.configFromZK(testCtx, constants.NcDiagDumpInterval)
		assert.NotNil(t, val)
		assert.Equal(t, "101", *val)
		val = zkCfg.configFromZK(testCtx, constants.ZkCustomConfig)
		assert.NotNil(t, val)
		assert.Equal(t, strings.TrimSpace(escConfig()), strings.TrimSpace(*val))
	})
}
