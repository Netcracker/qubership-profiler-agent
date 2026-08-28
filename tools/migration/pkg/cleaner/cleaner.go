package cleaner

import (
	"context"

	"github.com/Netcracker/qubership-profiler-backend/tools/migration/pkg/envconfig"
	"github.com/Netcracker/qubership-profiler-backend/libs/files"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type MigrationCleaner interface {
	// Workloads
	DeleteESCDeployments(ctx context.Context) error
	// Config
	DeleteESCConfigMaps(ctx context.Context) error
	DeleteESCSecrets(ctx context.Context) error
	// Service
	DeleteESCIngresses(ctx context.Context) error
	DeleteESCServices(ctx context.Context) error
	// Cluster
	DeleteESCClusterRoleBindings(ctx context.Context) error
	DeleteESCClusterRoles(ctx context.Context) error
	DeleteESCRoleBindings(ctx context.Context) error
	DeleteESCRoles(ctx context.Context) error
	DeleteESCServiceAccounts(ctx context.Context) error
	// Custom resource
	DeleteESCServiceMonitors(ctx context.Context) error
	DeleteESCGrafanaDashboards(ctx context.Context) error
	DeleteESCCertificates(ctx context.Context) error
}

type migrationCleaner struct {
	namespace     string
	labelSelector string
	kubeClientSet *kubernetes.Clientset
	dynamicClient *dynamic.DynamicClient
}

func NewMigrationCleaner(ctx context.Context) (MigrationCleaner, error) {
	var config *rest.Config
	var err error

	if envconfig.EnvConfig.Kubeconfig != "" {
		if err := files.CheckFile(envconfig.EnvConfig.Kubeconfig); err != nil {
			log.Error(ctx, err, "Kubeconfig file error")
			return nil, err
		}
		config, err = clientcmd.BuildConfigFromFlags("", envconfig.EnvConfig.Kubeconfig)
		if err != nil {
			log.Error(ctx, err, "Error initializing config for kube client using kubeconfig")
			return nil, err
		}
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			log.Error(ctx, err, "Error initializing in-cluster config for kube client")
			return nil, err
		}
	}

	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error(ctx, err, "Error initializing kubeclient")
		return nil, err
	}

	dynamicKubeClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Error(ctx, err, "Error initializing dynamic kubeclient")
		return nil, err
	}

	if envconfig.EnvConfig.Kubeconfig != "" {
		log.Info(ctx, "Initialized kubeclient, based on kubeconfig %s", envconfig.EnvConfig.Kubeconfig)
	} else {
		log.Info(ctx, "Initialized in-cluster kubeclient")
	}

	log.Info(ctx, "Used namespace \"%s\" and label selector \"%s\"",
		envconfig.EnvConfig.Namespace,
		envconfig.EnvConfig.LabelSelector)

	return &migrationCleaner{
		namespace:     envconfig.EnvConfig.Namespace,
		labelSelector: envconfig.EnvConfig.LabelSelector,
		kubeClientSet: clientSet,
		dynamicClient: dynamicKubeClient,
	}, nil
}
