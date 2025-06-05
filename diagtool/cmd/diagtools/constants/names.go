package constants

const (
	// Cloud Passport approved

	LogLevel            = "LOG_LEVEL"
	ZkEnabled           = "ZOOKEEPER_ENABLED" // disabled by default
	ZkAddress           = "ZOOKEEPER_ADDRESS"
	ConfigServerAddress = "CONFIG_SERVER"
	ConsulEnabled       = "CONSUL_ENABLED" // disabled by default
	ConsulAddress       = "CONSUL_URL"

	//  NOT(!) in Cloud Passport

	ESCLogLevel             = "ESC_LOG_LEVEL" // NOT(!) in Cloud Passport
	LogRetainInterval       = "KEEP_LOGS_INTERVAL"
	EnvLogFileSize          = "LOG_FILE_SIZE"
	EnvLogToConsole         = "LOG_TO_CONSOLE"
	NumberOfLogFileBackups  = "LOG_FILE_BACKUPS"
	NcDiagMode              = "QS_DIAGNOSTIC_MODE"
	NcDiagAgentService      = "QS_DIAGNOSTIC_AGENT_SERVICE"
	NcDiagnosticFolder      = "QS_DIAGNOSTIC_FOLDER"
	NcDiagLogFolder         = "QS_DIAGNOSTIC_LOGS_FOLDER"
	NcDiagJStackLongEnabled = "QS_DIAGNOSTIC_JSTACK_LONG_LISTENING_ENABLED" // long listening option for jstack (performance hit) // ENABLED by default
	NcDiagThreadDumpEnabled = "QS_DIAGNOSTIC_THREADDUMP_ENABLED"            // thread dump collection by scheduler in pod         // ENABLED by default
	NcDiagTopEnabled        = "QS_DIAGNOSTIC_TOP_ENABLED"                   // top collection by scheduler in pod                 // ENABLED by default
	NcDiagCenterDumpEnabled = "DIAGNOSTIC_CENTER_DUMPS_ENABLED"             // heap dump collection after OOM                     // ENABLED by default
	NcDiagDumpInterval      = "DIAGNOSTIC_DUMP_INTERVAL"
	NcDiagScanInterval      = "DIAGNOSTIC_SCAN_INTERVAL"
	NcProfilerFolder        = "PROFILER_FOLDER"
	ZipCompressionLevel     = "QS_HEAP_DUMP_COMPRESSION_LEVEL"
	NcCloudNamespace        = "CLOUD_NAMESPACE"
	EnvNamespace            = "NAMESPACE"
	KubeNamespaceFilePath   = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	NcMicroServiceName      = "MICROSERVICE_NAME"
	NcServiceName           = "SERVICE_NAME"
	IdpAddress              = "IDP_URL" // NOT(!) in Cloud Passport
	TlsEnabled              = "TLS_ENABLED"
)
