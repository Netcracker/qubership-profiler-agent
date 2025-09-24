package constants

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
)

var (
	isTest = false
)

func propFile(fileName string) string {
	if isTest {
		return filepath.FromSlash(fileName)
	}
	return filepath.Join(DefaultNcProfilerFolder, "properties", fileName)
}

func readFullFile(filePath string) (body []byte, err error) {
	body, err = os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}
	return body, err
}

func getProperty(ctx context.Context, propertyName string) bool {
	// Check file first
	if fileVal, fileErr := readFullFile(propFile(propertyName)); fileErr == nil {
		if fileVal, fileErr := strconv.ParseBool(string(fileVal)); fileErr == nil {
			return fileVal
		} else {
			log.Errorf(ctx, fileErr, "Error while parsing value from file")
		}
	} else {
		log.Debugf(ctx, "Error while reading value from file %s", propertyName)
	}
	// Then check environment variable
	if val, err := strconv.ParseBool(os.Getenv(propertyName)); err == nil {
		return val
	}
	// ENABLED by default
	return true
}

func GetLogLevelFromEnv() (logLevelEnv string) {
	logLevelEnv = os.Getenv(ESCLogLevel)
	if len(logLevelEnv) == 0 {
		// try to use level from application itself, but skip debug/trace levels
		logLevelEnv = os.Getenv(LogLevel)
		if len(logLevelEnv) == 0 {
			logLevelEnv = "info"
		} else if strings.ToLower(logLevelEnv) == "debug" {
			logLevelEnv = "info"
		} else if strings.ToLower(logLevelEnv) == "trace" {
			logLevelEnv = "info"
		}
	}
	return strings.ToLower(logLevelEnv)
}

func DiagService(ctx context.Context) string {
	diagService := strings.TrimSpace(os.Getenv(NcDiagAgentService))
	tlsEnabled := strings.TrimSpace(os.Getenv(TlsEnabled))
	namespace, _ := GetNamespace()
	NcDiagServiceUrlDefaultPort := ""
	if diagService == "" {
		diagService = DefaultNcDiagAgentService
	}
	if tlsEnabled == "true" {
		NcDiagServiceUrlDefaultPort = NcDiagServiceUrlDefaultHttpsPort
		if strings.HasPrefix(diagService, "http:") {
			log.Errorf(ctx, fmt.Errorf("HTTP protocol used instead of HTTPS when TLS is enabled"),
				"Error while parsing NC_DIAGNOSTIC_AGENT_SERVICE")
			return diagService
		} else {
			if !strings.HasPrefix(diagService, "https:") {
				diagService = NcDiagServiceUrlSchemeHttps + diagService
			}
		}
	} else {
		NcDiagServiceUrlDefaultPort = NcDiagServiceUrlDefaultHttpPort
		if strings.HasPrefix(diagService, "https:") {
			log.Errorf(ctx, fmt.Errorf("HTTPS protocol used instead of HTTP when TLS is disabled"),
				"Error while parsing NC_DIAGNOSTIC_AGENT_SERVICE")
			return diagService
		} else {
			if !strings.HasPrefix(diagService, "http:") {
				diagService = NcDiagServiceUrlSchemeHttp + diagService
			}
		}
	}

	if m, _ := regexp.MatchString(".:\\d+$", diagService); !m {
		diagService = diagService + NcDiagServiceUrlDefaultPort
	}

	namespaceAbsent := len(strings.Split(diagService, ".")) <= 1
	if namespaceAbsent {
		re := regexp.MustCompile(`:(\d+)$`)
		matchedUrlArrray := re.FindStringSubmatch(diagService)
		baseUrl := diagService[:len(diagService)-len(matchedUrlArrray[0])]
		port := matchedUrlArrray[1]
		diagService = baseUrl + "." + namespace + ":" + port
	}

	return diagService
}

func GetNamespace() (string, error) {
	namespace, _ := os.LookupEnv(NcCloudNamespace)
	if namespace == "" {
		namespace, _ = os.LookupEnv(EnvNamespace)
		if namespace == "" {
			NamespaceContents, err := os.ReadFile(KubeNamespaceFilePath)
			if err != nil {
				return "unknown", fmt.Errorf("could not determine the namespace: %w", err)
			}
			if len(NamespaceContents) == 0 {
				return "unknown", fmt.Errorf("could not read the namespace: empty namespace file")
			}
			namespace = string(NamespaceContents)
		}
	}
	return namespace, nil
}

// IsDcdEnabled is Heap Dump collection enabled? (after OOM)
func IsDcdEnabled() bool {
	if val, err := strconv.ParseBool(os.Getenv(NcDiagCenterDumpEnabled)); err == nil {
		return val
	}
	// ENABLED by default -- will add jvm-args to gather heap dumps after OOM
	return true
}

// IsLongListeningEnabled is  long listening option for jstack enabled? (cause performance hit)
func IsLongListeningEnabled() bool {
	if val, err := strconv.ParseBool(os.Getenv(NcDiagJStackLongEnabled)); err == nil {
		return val
	}
	// DISABLED by default
	return false
}

// LongListeningArgs jstack args with long listening option
func LongListeningArgs() string {
	if IsLongListeningEnabled() {
		return "-l"
	}
	return ""
}

// IsTopDumpEnabled is top dump collection enabled?  (every minute, by scheduler)
func IsTopDumpEnabled(ctx context.Context) bool {
	return getProperty(ctx, NcDiagTopEnabled)
}

// IsThreadDumpEnabled is thread dump collection enabled?  (every minute, by scheduler)
func IsThreadDumpEnabled(ctx context.Context) bool {
	return getProperty(ctx, NcDiagThreadDumpEnabled)
}

// IsZookeeperEnabled is integration with Zookeeper enabled? (to load Profiler Agent configuration for the service)
func IsZookeeperEnabled() bool {
	if val, err := strconv.ParseBool(os.Getenv(ZkEnabled)); err == nil {
		return val
	}
	// DISABLED by default
	return false
}

// IsConsulEnabled is integration with Consul enabled? (to load Profiler Agent configuration for the service)
func IsConsulEnabled() bool {
	if val, err := strconv.ParseBool(os.Getenv(ConsulEnabled)); err == nil {
		return val
	}
	url := strings.TrimSpace(os.Getenv(ConsulAddress))
	return len(url) > 0
}

// ConsulUrl Consul address
func ConsulUrl() (url string, err error) {
	url = strings.TrimSpace(os.Getenv(ConsulAddress))
	if len(url) == 0 {
		err = errors.New(ConsulAddress + " is empty")
	}
	return
}

// IsConfigServerEnabled is integration with ConfigServer enabled? (to load Profiler Agent configuration for the service)
func IsConfigServerEnabled() bool {
	url, err := ConfigServerUrl()
	return len(url) > 0 && err == nil // DISABLED by default
}

// ConfigServerUrl ConfigServer address
func ConfigServerUrl() (configServerUrl string, err error) {
	configServerUrl = strings.TrimSpace(os.Getenv(ConfigServerAddress))
	if len(configServerUrl) == 0 {
		err = errors.New(ConfigServerAddress + " is empty")
	} else {
		_, err = url.ParseRequestURI(configServerUrl)
		if err != nil {
			err = errors.Join(err, fmt.Errorf("invalid url: %s", configServerUrl))
		}
	}
	return
}

// IdpUrl IDP address
func IdpUrl() (idpUrl string, err error) {
	idpUrl = strings.TrimSpace(os.Getenv(IdpAddress))
	if len(idpUrl) == 0 {
		idpUrl = DefaultIdpUrl
	}
	_, err = url.ParseRequestURI(idpUrl)
	if err != nil {
		return "", errors.Join(err, fmt.Errorf("invalid idp url: %s", idpUrl))
	}
	return
}

func DiagFolder() string {
	diagFolder, found := os.LookupEnv(NcDiagnosticFolder)
	if !found {
		diagFolder = DefaultNcProfilerFolder
	}
	return diagFolder
}

func LogFolder() string {
	logFolder := os.Getenv(NcDiagLogFolder)
	if logFolder == "" {
		logFolder = DefaultNcDiagLogFolder
	}
	return logFolder
}

func LogPrintToConsole() bool {
	logToConsole := false
	val := os.Getenv(EnvLogToConsole)
	if val != "" {
		var err error
		logToConsole, err = strconv.ParseBool(val)
		if err != nil {
			fmt.Printf("Fail parsing '%s' ('%v'): %s. Use default value 'false'",
				EnvLogToConsole, val, err.Error())
			logToConsole = false
		}
	}
	return logToConsole
}

func LogFileSize() int {
	logFileSize := DefaultLogFileSizeMb
	val := os.Getenv(EnvLogFileSize)
	if val != "" {
		var err error
		logFileSize, err = strconv.Atoi(val)
		if err != nil {
			fmt.Printf("parse %s failed: %s. Default value - %d mb will be used.",
				EnvLogFileSize, err.Error(), DefaultLogFileSizeMb)
			logFileSize = DefaultLogFileSizeMb
		}
	}
	return logFileSize
}

func LogFileBackups() int {
	logFileBackups := DefaultNumberOfLogFileBackups
	val := os.Getenv(NumberOfLogFileBackups)
	if val != "" {
		var err error
		logFileBackups, err = strconv.Atoi(val)
		if err != nil {
			fmt.Printf("parse %s failed: %s. Default value - %d files will be used",
				NumberOfLogFileBackups, err.Error(), DefaultNumberOfLogFileBackups)
			logFileBackups = DefaultNumberOfLogFileBackups
		}
	}
	return logFileBackups
}

func LogInterval() int {
	logInterval := DefaultLogRetainIntervalInDays
	val := os.Getenv(LogRetainInterval)
	if val != "" {
		var err error
		logInterval, err = strconv.Atoi(val)
		if err != nil {
			fmt.Printf("parse %s failed: %s. Default value - %d days will be used.",
				LogRetainInterval, err.Error(), DefaultLogRetainIntervalInDays)
			logInterval = DefaultLogRetainIntervalInDays
		}
	}
	return logInterval
}

func DumpFolder() string {
	dumpFolder := os.Getenv(NcDiagLogFolder)
	if dumpFolder == "" {
		dumpFolder = DefaultNcDumpFolder
	}
	return dumpFolder
}

func DumpInterval(ctx context.Context) time.Duration {
	dumpIntervalEnv := os.Getenv(NcDiagDumpInterval)
	if dumpIntervalEnv == "" {
		def := DefaultNcDiagDumpInterval
		log.Infof(ctx, "%s is empty. Will use default value: '%s'", NcDiagDumpInterval, def)
		dumpIntervalEnv = def
	}

	dumpInterval, err := time.ParseDuration(dumpIntervalEnv)
	if err != nil {
		log.Error(ctx, err, "Parsing dump interval failed.  Will use default value: 2 minute")
		dumpInterval = 2 * time.Minute
	}
	return dumpInterval
}

func ScanInterval(ctx context.Context) time.Duration {
	scanIntervalEnv := os.Getenv(NcDiagScanInterval)
	if scanIntervalEnv == "" {
		def := DefaultNcDiagScanInterval
		log.Infof(ctx, "%s is empty. Will use default value '%s'", NcDiagScanInterval, def)
		scanIntervalEnv = DefaultNcDiagScanInterval
	}

	scanInterval, err := time.ParseDuration(scanIntervalEnv)
	if err != nil {
		log.Error(ctx, err, "Parsing scan interval failed. Will use default value: 3 minutes")
		scanInterval = 3 * time.Minute
	}
	return scanInterval
}
