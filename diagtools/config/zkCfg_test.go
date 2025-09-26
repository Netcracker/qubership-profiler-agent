package config

import (
	"context"
	"os"
	"testing"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
	"github.com/stretchr/testify/assert"
)

const (
	testDir = "../../.docker/tmp"
)

var (
	testCtx = context.Background()
)

func init() {
	log.SetupTestLogger()
}

func TestZkCfg_InitConfig(t *testing.T) {
	os.Setenv(constants.NcCloudNamespace, "ns")
	os.Setenv(constants.NcMicroServiceName, "svc")

	os.Setenv(constants.ZkAddress, "localhost:2190,localhost:2192") // invalid ports
	t.Run("invalid port", func(t *testing.T) {
		zkCfg := &ZkCfg{}
		err := zkCfg.InitConfig(testCtx)
		assert.Nil(t, err)
		assert.Equal(t, "ns", zkCfg.Namespace)
		assert.Equal(t, "svc", zkCfg.ServiceName)
		assert.Equal(t, []string{"localhost:2190", "localhost:2192"}, zkCfg.ZkAddresses)
	})

	os.Setenv(constants.NcCloudNamespace, "ns2")
	os.Setenv(constants.NcMicroServiceName, "svc2")
	os.Setenv(constants.ZkAddress, "localhost:2181")

	t.Run("docker", func(t *testing.T) {
		zkCfg := &ZkCfg{}
		assert.Nil(t, zkCfg.getServiceInfo(testCtx))

		err := zkCfg.InitConfig(testCtx)
		assert.Nil(t, err)
		assert.Equal(t, "ns2", zkCfg.Namespace)
		assert.Equal(t, "svc2", zkCfg.ServiceName)
		assert.Equal(t, []string{"localhost:2181"}, zkCfg.ZkAddresses)
	})
}
