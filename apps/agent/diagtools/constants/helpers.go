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

// IsGcLogEnabled is GC log collection enabled? (every scan interval, by scheduler)
func IsGcLogEnabled(ctx context.Context) bool {
	return getProperty(ctx, NcDiagGcEnabled)
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

	dumpInterval, err := parseDurationOrSeconds(dumpIntervalEnv)
	if err != nil {
		log.Error(ctx, err, "Parsing dump interval failed.  Will use default value: 2 minute")
		dumpInterval = 2 * time.Minute
	}
	return dumpInterval
}

func ScanInterval(ctx context.Context) time.Duration {
	scanIntervalEnv := os.Getenv(NcDiagScanInterval)
	if scanIntervalEnv == "" {
		dumpIntervalEnv := os.Getenv(NcDiagDumpInterval)
		if dumpIntervalEnv != "" {
			log.Infof(ctx, "%s is empty. Will use '%s' value '%s'", NcDiagScanInterval, NcDiagDumpInterval, dumpIntervalEnv)
			scanIntervalEnv = dumpIntervalEnv
		} else {
			def := DefaultNcDiagScanInterval
			log.Infof(ctx, "%s is empty. Will use default value '%s'", NcDiagScanInterval, def)
			scanIntervalEnv = DefaultNcDiagScanInterval
		}
	}

	scanInterval, err := parseDurationOrSeconds(scanIntervalEnv)
	if err != nil {
		log.Error(ctx, err, "Parsing scan interval failed. Will use default value: 3 minutes")
		scanInterval = 3 * time.Minute
	}
	return scanInterval
}

func parseDurationOrSeconds(interval string) (time.Duration, error) {
	duration, err := time.ParseDuration(interval)
	if err == nil {
		return duration, nil
	}

	seconds, errSeconds := strconv.Atoi(interval)
	if errSeconds == nil {
		return time.Duration(seconds) * time.Second, nil
	}

	return 0, err
}

// UploadTimeout returns the HTTP client timeout for sending dump files (e.g. to dumps-collector).
// Configured via DIAGNOSTIC_UPLOAD_TIMEOUT; supports duration strings (e.g. "30m", "1h") or seconds as integer.
// Default is 5 minutes.
func UploadTimeout(ctx context.Context) time.Duration {
	env := os.Getenv(NcDiagUploadTimeout)
	if env == "" {
		return 5 * time.Minute
	}
	d, err := parseDurationOrSeconds(env)
	if err != nil {
		log.Error(ctx, err, "Parsing upload timeout failed. Will use default: 5m")
		return 5 * time.Minute
	}
	return d
}

// UploadMaxAge returns how long a pending dump file is allowed to sit on disk
// before the scheduled cleanup removes it. Configured via DIAGNOSTIC_UPLOAD_MAX_AGE
// using Go duration syntax (e.g. "48h", "30m"). Default is 48 hours.
func UploadMaxAge(ctx context.Context) time.Duration {
	env := os.Getenv(DiagnosticUploadMaxAge)
	if env == "" {
		return DefaultDiagnosticUploadMaxAge
	}
	d, err := parseDurationOrSeconds(env)
	if err != nil {
		log.Errorf(ctx, err, "Parsing %s=%q failed. Will use default: %s",
			DiagnosticUploadMaxAge, env, DefaultDiagnosticUploadMaxAge)
		return DefaultDiagnosticUploadMaxAge
	}
	return d
}

// PendingMaxBytes returns the cap on combined size of pending dump files.
// Configured via DIAGNOSTIC_PENDING_MAX_BYTES; accepts SI suffixes K/M/G/T (×1000)
// and binary suffixes Ki/Mi/Gi/Ti (×1024). A bare integer is interpreted as bytes.
// Default is 10 GiB.
func PendingMaxBytes(ctx context.Context) int64 {
	env := os.Getenv(DiagnosticPendingMaxBytes)
	if env == "" {
		return DefaultDiagnosticPendingMaxBytes
	}
	n, err := parseByteSize(env)
	if err != nil {
		log.Errorf(ctx, err, "Parsing %s=%q failed. Will use default: %d bytes",
			DiagnosticPendingMaxBytes, env, DefaultDiagnosticPendingMaxBytes)
		return DefaultDiagnosticPendingMaxBytes
	}
	return n
}

var byteSizeRE = regexp.MustCompile(`^\s*([0-9]+)\s*([KkMmGgTt][Ii]?[Bb]?)?\s*$`)

func parseByteSize(s string) (int64, error) {
	m := byteSizeRE.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("invalid byte size: %q", s)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size number in %q: %w", s, err)
	}
	suffix := strings.TrimSuffix(strings.ToUpper(m[2]), "B")
	var mult int64
	switch suffix {
	case "":
		mult = 1
	case "K":
		mult = 1000
	case "M":
		mult = 1000 * 1000
	case "G":
		mult = 1000 * 1000 * 1000
	case "T":
		mult = 1000 * 1000 * 1000 * 1000
	case "KI":
		mult = 1024
	case "MI":
		mult = 1024 * 1024
	case "GI":
		mult = 1024 * 1024 * 1024
	case "TI":
		mult = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unsupported byte size suffix in %q", s)
	}
	if n > 0 && mult > 0 && n > (1<<62)/mult {
		return 0, fmt.Errorf("byte size overflow: %q", s)
	}
	return n * mult, nil
}
