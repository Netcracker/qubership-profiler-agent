package constants

import "path/filepath"

// datetime formats
const (
	DumpFileTimestampLayout = "20060102T150405"
	NcDiagDateLayout        = "2006/01/02/15/04/05"
)

// default params
const (
	DefaultLogRetainIntervalInDays = 2
	DefaultLogFileSizeMb           = 1
	DefaultNumberOfLogFileBackups  = 5
	DefaultNcDiagDumpInterval      = "1m"
	DefaultNcDiagScanInterval      = "1m"
)

// default external services
const (
	DefaultNcDiagAgentService        = "nc-diagnostic-agent"
	NcDiagServiceUrlSchemeHttp       = "http://"
	NcDiagServiceUrlSchemeHttps      = "https://"
	NcDiagServiceUrlDefaultHttpPort  = ":8080"
	NcDiagServiceUrlDefaultHttpsPort = ":8443"
	ConsulAclPath                    = "/v1/acl/login"
	ConsulDC                         = "dc1"
	DefaultIdpUrl                    = "http://identity-provider:8080"
	TokenPath                        = "/auth/realms/cloud-common/protocol/openid-connect/token" // get token from idp
)

const (
	NcDiagLogSubFolder      = "log"
	DefaultNcDiagLogFolder  = "/tmp/diagnostic"
	DefaultNcDumpFolder     = "/tmp/diagnostic"
	NcDiagServicePath       = "diagnostic"
	DefaultNcProfilerFolder = "/app/ncdiag"
	DefaultPodNameFile      = "pod.name"
	ZkConfigPrefix          = "/config"
	ZkConfigXmlFilePath     = "/default/zookeeper.xml"
	ServerAppPath           = "application"
	ZkCustomConfig          = "esc.config"
	CustomConfigXmlFilePath = "/config/default/00-custom-config.xml"
	DumpFileSuffix          = ".hprof"
	DumpFilePattern         = "*.hprof*"
	ThreadDumpSuffix        = ".td.txt"
	TopDumpSuffix           = ".top.txt"
	UserSecret              = "/etc/secret/username"
	PswdSecret              = "/etc/secret/password"
)

var (
	DefaultPodNameFilePath = filepath.Join(DefaultNcProfilerFolder, DefaultPodNameFile)
)
