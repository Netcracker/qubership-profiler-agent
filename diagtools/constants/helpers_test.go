package constants

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"

	"github.com/stretchr/testify/assert"
)

var (
	testCtx = context.Background()
)

func init() {
	log.SetupTestLogger()
	isTest = true
}

func saveFile(property string, val []byte) error {
	if val == nil {
		return nil
	}
	if len(val) == 0 {

		return nil
	}
	target := propFile(property)
	err := os.WriteFile(target, val, 0644)
	if err != nil {
		log.Errorf(testCtx, err, "failed to save file")
	}
	return err
}

func deleteFile(property string) error {
	target := propFile(property)
	err := os.Remove(target)
	if err != nil {
		log.Errorf(testCtx, err, "failed to delete file")
	}
	return err
}

func TestIsDcdEnabled(t *testing.T) {
	assert.True(t, IsDcdEnabled())
	os.Setenv("DIAGNOSTIC_CENTER_DUMPS_ENABLED", "true")
	assert.True(t, IsDcdEnabled())
	os.Setenv("DIAGNOSTIC_CENTER_DUMPS_ENABLED", "false")
	assert.False(t, IsDcdEnabled())
	os.Setenv(NcDiagCenterDumpEnabled, "true")
	assert.True(t, IsDcdEnabled())
	assert.Equal(t, "DIAGNOSTIC_CENTER_DUMPS_ENABLED", NcDiagCenterDumpEnabled)
}

func TestIsLongListeningEnabled(t *testing.T) {
	assert.False(t, IsLongListeningEnabled())
	os.Setenv("NC_DIAGNOSTIC_JSTACK_LONG_LISTENING_ENABLED", "true")
	assert.True(t, IsLongListeningEnabled())
	os.Setenv("NC_DIAGNOSTIC_JSTACK_LONG_LISTENING_ENABLED", "false")
	assert.False(t, IsLongListeningEnabled())
	os.Setenv(NcDiagJStackLongEnabled, "true")
	assert.True(t, IsLongListeningEnabled())
	assert.Equal(t, "NC_DIAGNOSTIC_JSTACK_LONG_LISTENING_ENABLED", NcDiagJStackLongEnabled)
}

func TestIsThreadDumpEnabled(t *testing.T) {
	assert.True(t, IsThreadDumpEnabled(testCtx))
	os.Setenv("NC_DIAGNOSTIC_THREADDUMP_ENABLED", "true")
	assert.True(t, IsThreadDumpEnabled(testCtx))
	os.Setenv("NC_DIAGNOSTIC_THREADDUMP_ENABLED", "false")
	assert.False(t, IsThreadDumpEnabled(testCtx))
	os.Setenv(NcDiagThreadDumpEnabled, "true")
	saveFile("NC_DIAGNOSTIC_THREADDUMP_ENABLED", []byte("true"))
	assert.True(t, IsThreadDumpEnabled(testCtx))
	saveFile("NC_DIAGNOSTIC_THREADDUMP_ENABLED", []byte("false"))
	assert.False(t, IsThreadDumpEnabled(testCtx))
	deleteFile("NC_DIAGNOSTIC_THREADDUMP_ENABLED")
	assert.True(t, IsThreadDumpEnabled(testCtx))
	assert.Equal(t, "NC_DIAGNOSTIC_THREADDUMP_ENABLED", NcDiagThreadDumpEnabled)
}

func TestIsTopDumpEnabled(t *testing.T) {
	assert.True(t, IsTopDumpEnabled(testCtx))
	os.Setenv("NC_DIAGNOSTIC_TOP_ENABLED", "true")
	assert.True(t, IsTopDumpEnabled(testCtx))
	os.Setenv("NC_DIAGNOSTIC_TOP_ENABLED", "false")
	assert.False(t, IsTopDumpEnabled(testCtx))
	saveFile("NC_DIAGNOSTIC_TOP_ENABLED", []byte("true"))
	assert.True(t, IsTopDumpEnabled(testCtx))
	saveFile("NC_DIAGNOSTIC_TOP_ENABLED", []byte("false"))
	assert.False(t, IsTopDumpEnabled(testCtx))
	deleteFile("NC_DIAGNOSTIC_TOP_ENABLED")
	os.Setenv(NcDiagTopEnabled, "true")
	assert.True(t, IsTopDumpEnabled(testCtx))
	assert.Equal(t, "NC_DIAGNOSTIC_TOP_ENABLED", NcDiagTopEnabled)
}

func TestIsZookeeperEnabled(t *testing.T) {
	assert.False(t, IsZookeeperEnabled()) // disabled by default
	os.Setenv("ZOOKEEPER_ENABLED", "true")
	assert.True(t, IsZookeeperEnabled())
	os.Setenv("ZOOKEEPER_ENABLED", "false")
	assert.False(t, IsZookeeperEnabled())
	os.Setenv(ZkEnabled, "true")
	assert.True(t, IsZookeeperEnabled())
	assert.Equal(t, "ZOOKEEPER_ENABLED", ZkEnabled)
}

func TestIsConfigServerEnabled(t *testing.T) {
	assert.False(t, IsConfigServerEnabled()) // disabled by default
	os.Setenv("CONFIG_SERVER", "http://localhost")
	assert.True(t, IsConfigServerEnabled())
	os.Setenv("CONFIG_SERVER", "")
	assert.False(t, IsConfigServerEnabled())
	os.Setenv(ConfigServerAddress, "http://localhost")
	assert.True(t, IsConfigServerEnabled())
	assert.Equal(t, "CONFIG_SERVER", ConfigServerAddress)
	os.Setenv("CONFIG_SERVER", "")
}

func TestConfigServerUrl(t *testing.T) {
	url, err := ConfigServerUrl()
	assert.Equal(t, "", url) // disabled by default
	assert.NotNil(t, err)

	os.Setenv("CONFIG_SERVER", "  http://localhost  ")
	url, err = ConfigServerUrl()
	assert.Equal(t, "http://localhost", url)
	assert.Nil(t, err)

	os.Setenv("CONFIG_SERVER", "  localhost  ")
	url, err = ConfigServerUrl()
	assert.Equal(t, "localhost", url)
	assert.NotNil(t, err)

	os.Setenv("CONFIG_SERVER", "               ")
	url, err = ConfigServerUrl()
	assert.Equal(t, "", url)
	assert.NotNil(t, err)

	os.Setenv(ConfigServerAddress, "http://localhost")
	assert.True(t, IsConfigServerEnabled())
	url, err = ConfigServerUrl()
	assert.Equal(t, "http://localhost", url)
	assert.Nil(t, err)

	assert.Equal(t, "CONFIG_SERVER", ConfigServerAddress)
}

func TestIsConsulEnabled(t *testing.T) {
	assert.False(t, IsConsulEnabled()) // disabled by default
	os.Setenv("CONSUL_ENABLED", "true")
	assert.True(t, IsConsulEnabled())
	os.Setenv("CONSUL_ENABLED", "false")
	assert.False(t, IsConsulEnabled())
	os.Setenv(ConsulEnabled, "true")
	assert.True(t, IsConsulEnabled())
	assert.Equal(t, "CONSUL_ENABLED", ConsulEnabled)
}

func TestConsulUrl(t *testing.T) {
	url, err := ConsulUrl()
	assert.Equal(t, "", url) // disabled by default
	assert.NotNil(t, err)

	os.Setenv("CONSUL_URL", "  localhost2  ")
	url, err = ConsulUrl()
	assert.Equal(t, "localhost2", url)
	assert.Nil(t, err)

	os.Setenv("CONSUL_URL", "               ")
	url, err = ConsulUrl()
	assert.Equal(t, "", url)
	assert.NotNil(t, err)

	os.Setenv(ConsulAddress, "localhost2")
	url, err = ConsulUrl()
	assert.Equal(t, "localhost2", url)
	assert.Nil(t, err)

	assert.Equal(t, "CONSUL_URL", ConsulAddress)
}

func TestIdpUrl(t *testing.T) {
	url, err := IdpUrl()
	assert.Equal(t, "http://identity-provider:8080", url) // default core url
	assert.Nil(t, err)

	os.Setenv("IDP_URL", "  http://localhost2:8090  ")
	url, err = IdpUrl()
	assert.Equal(t, "http://localhost2:8090", url)
	assert.Nil(t, err)

	os.Setenv("IDP_URL", "  https://localhost2  ")
	url, err = IdpUrl()
	assert.Equal(t, "https://localhost2", url)
	assert.Nil(t, err)

	os.Setenv("IDP_URL", "  localhost2  ")
	url, err = IdpUrl()
	assert.Equal(t, "", url)
	assert.NotNil(t, err)

	os.Setenv("IDP_URL", "               ")
	url, err = IdpUrl()
	assert.Equal(t, "http://identity-provider:8080", url)
	assert.Nil(t, err)

	os.Setenv(IdpAddress, "localhost2")
	url, err = IdpUrl()
	assert.Equal(t, "", url)
	assert.NotNil(t, err)

	assert.Equal(t, "IDP_URL", IdpAddress)
}

func TestDiagFolder(t *testing.T) {
	assert.Equal(t, "/app/ncdiag", DiagFolder())
	os.Setenv("NC_DIAGNOSTIC_FOLDER", "/app/ncdiag/test")
	assert.Equal(t, "/app/ncdiag/test", DiagFolder())
}

func TestScanInterval(t *testing.T) {
	assert.Equal(t, 1*time.Minute, ScanInterval(testCtx)) // default interval
	os.Setenv("DIAGNOSTIC_SCAN_INTERVAL", "10s")
	assert.Equal(t, 10*time.Second, ScanInterval(testCtx))
	os.Setenv("DIAGNOSTIC_SCAN_INTERVAL", "7m")
	assert.Equal(t, 7*time.Minute, ScanInterval(testCtx))
	os.Setenv("DIAGNOSTIC_SCAN_INTERVAL", "2h")
	assert.Equal(t, 2*time.Hour, ScanInterval(testCtx))
	os.Setenv("DIAGNOSTIC_SCAN_INTERVAL", "bad")
	assert.Equal(t, 3*time.Minute, ScanInterval(testCtx)) // default interval for invalid values
	os.Setenv(NcDiagScanInterval, "2m")
	assert.Equal(t, 2*time.Minute, ScanInterval(testCtx))
	assert.Equal(t, "DIAGNOSTIC_SCAN_INTERVAL", NcDiagScanInterval)
}

func TestDumpFolder(t *testing.T) {
	assert.Equal(t, "/tmp/diagnostic", DumpFolder())
	os.Setenv("NC_DIAGNOSTIC_LOGS_FOLDER", "/tmp/diagnostic/test")
	assert.Equal(t, "/tmp/diagnostic/test", DumpFolder())
	os.Setenv("NC_DIAGNOSTIC_LOGS_FOLDER", "")
}

func TestDumpInterval(t *testing.T) {
	assert.Equal(t, 1*time.Minute, DumpInterval(testCtx)) // default interval
	os.Setenv("DIAGNOSTIC_DUMP_INTERVAL", "10s")
	assert.Equal(t, 10*time.Second, DumpInterval(testCtx))
	os.Setenv("DIAGNOSTIC_DUMP_INTERVAL", "7m")
	assert.Equal(t, 7*time.Minute, DumpInterval(testCtx))
	os.Setenv("DIAGNOSTIC_DUMP_INTERVAL", "2h")
	assert.Equal(t, 2*time.Hour, DumpInterval(testCtx))
	os.Setenv("DIAGNOSTIC_DUMP_INTERVAL", "bad")
	assert.Equal(t, 2*time.Minute, DumpInterval(testCtx)) // default interval for invalid values
	os.Setenv(NcDiagDumpInterval, "6m")
	assert.Equal(t, 6*time.Minute, DumpInterval(testCtx))
	assert.Equal(t, "DIAGNOSTIC_DUMP_INTERVAL", NcDiagDumpInterval)
}

func TestLogFolder(t *testing.T) {
	os.Setenv("NC_DIAGNOSTIC_LOGS_FOLDER", "")
	assert.Equal(t, "/tmp/diagnostic", LogFolder())
	os.Setenv("NC_DIAGNOSTIC_LOGS_FOLDER", "/tmp/diagnostic/test")
	assert.Equal(t, "/tmp/diagnostic/test", LogFolder())
}

func TestLogInterval(t *testing.T) {
	assert.Equal(t, 2, LogInterval())    // default interval
	os.Setenv("KEEP_LOGS_INTERVAL", "3") // days
	assert.Equal(t, 3, LogInterval())
	os.Setenv("KEEP_LOGS_INTERVAL", "bad")
	assert.Equal(t, 2, LogInterval()) // default interval for invalid values
	os.Setenv(LogRetainInterval, "6")
	assert.Equal(t, 6, LogInterval())
	assert.Equal(t, "KEEP_LOGS_INTERVAL", LogRetainInterval)
}

func TestLogPrintToConsole(t *testing.T) {
	assert.Equal(t, false, LogPrintToConsole()) // default
	os.Setenv("LOG_TO_CONSOLE", "true")
	assert.Equal(t, true, LogPrintToConsole())
	os.Setenv("LOG_TO_CONSOLE", "TRUE")
	assert.Equal(t, true, LogPrintToConsole())
	os.Setenv("LOG_TO_CONSOLE", "false")
	assert.Equal(t, false, LogPrintToConsole())
	os.Setenv("LOG_TO_CONSOLE", "bad")
	assert.Equal(t, false, LogPrintToConsole()) // default for invalid values
	os.Setenv(EnvLogToConsole, "true")
	assert.Equal(t, true, LogPrintToConsole())
	assert.Equal(t, "LOG_TO_CONSOLE", EnvLogToConsole)
}

func TestLogFileSize(t *testing.T) {
	assert.Equal(t, 1, LogFileSize()) // default size in mb
	os.Setenv("LOG_FILE_SIZE", "3")   // days
	assert.Equal(t, 3, LogFileSize())
	os.Setenv("LOG_FILE_SIZE", "bad")
	assert.Equal(t, 1, LogFileSize()) // default for invalid values
	os.Setenv(EnvLogFileSize, "6")
	assert.Equal(t, 6, LogFileSize())
	assert.Equal(t, "LOG_FILE_SIZE", EnvLogFileSize)
}

func TestLogFileBackups(t *testing.T) {
	assert.Equal(t, 5, LogFileBackups()) // default size in mb
	os.Setenv("LOG_FILE_BACKUPS", "3")   // days
	assert.Equal(t, 3, LogFileBackups())
	os.Setenv("LOG_FILE_BACKUPS", "bad")
	assert.Equal(t, 5, LogFileBackups()) // default for invalid values
	os.Setenv(NumberOfLogFileBackups, "6")
	assert.Equal(t, 6, LogFileBackups())
	assert.Equal(t, "LOG_FILE_BACKUPS", NumberOfLogFileBackups)
}

func TestGetLogLevelFromEnv(t *testing.T) {
	os.Setenv("ESC_LOG_LEVEL", "")
	os.Setenv("LOG_LEVEL", "")
	assert.Equal(t, "info", GetLogLevelFromEnv())

	os.Setenv("ESC_LOG_LEVEL", "OFF")
	os.Setenv("LOG_LEVEL", "")
	assert.Equal(t, "off", GetLogLevelFromEnv())

	os.Setenv("ESC_LOG_LEVEL", "ERROR")
	os.Setenv("LOG_LEVEL", "")
	assert.Equal(t, "error", GetLogLevelFromEnv())

	os.Setenv("ESC_LOG_LEVEL", "WARN")
	os.Setenv("LOG_LEVEL", "")
	assert.Equal(t, "warn", GetLogLevelFromEnv())

	os.Setenv("ESC_LOG_LEVEL", "INFO")
	os.Setenv("LOG_LEVEL", "")
	assert.Equal(t, "info", GetLogLevelFromEnv())

	os.Setenv("ESC_LOG_LEVEL", "DEBUG")
	os.Setenv("LOG_LEVEL", "")
	assert.Equal(t, "debug", GetLogLevelFromEnv())

	os.Setenv("ESC_LOG_LEVEL", "TRACE")
	os.Setenv("LOG_LEVEL", "")
	assert.Equal(t, "trace", GetLogLevelFromEnv())

	os.Setenv("ESC_LOG_LEVEL", "")
	os.Setenv("LOG_LEVEL", "OFF")
	assert.Equal(t, "off", GetLogLevelFromEnv())

	os.Setenv("ESC_LOG_LEVEL", "")
	os.Setenv("LOG_LEVEL", "ERROR")
	assert.Equal(t, "error", GetLogLevelFromEnv())

	os.Setenv("ESC_LOG_LEVEL", "")
	os.Setenv("LOG_LEVEL", "WARN")
	assert.Equal(t, "warn", GetLogLevelFromEnv())

	os.Setenv("ESC_LOG_LEVEL", "")
	os.Setenv("LOG_LEVEL", "INFO")
	assert.Equal(t, "info", GetLogLevelFromEnv())

	os.Setenv("ESC_LOG_LEVEL", "")
	os.Setenv("LOG_LEVEL", "DEBUG")
	assert.Equal(t, "info", GetLogLevelFromEnv())

	os.Setenv("ESC_LOG_LEVEL", "")
	os.Setenv("LOG_LEVEL", "TRACE")
	assert.Equal(t, "info", GetLogLevelFromEnv())

}
