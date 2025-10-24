package envconfig

import "github.com/kelseyhightower/envconfig"

type Config struct {
	// Log level
	LogLevel string `envconfig:"LOG_LEVEL" default:"info"`
	// Kube settings
	Kubeconfig       string `envconfig:"KUBECONFIG"`
	Namespace        string `envconfig:"NAMESPACE" default:"profiler"`
	LabelSelector    string `envconfig:"ESC_LABEL" default:"app.kubernetes.io/part-of=esc"`
	PrivilegedRights bool   `envconfig:"PRIVILEGED_RIGHTS" default:"true"`
}

var EnvConfig Config

func InitConfig() error {
	return envconfig.Process("", &EnvConfig)
}
