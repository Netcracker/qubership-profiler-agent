package cleaner

import (
	"context"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	serviceMonitorGroup    = "monitoring.coreos.com"
	serviceMonitorResource = "servicemonitors"
	serviceMonitorVersion  = "v1"

	grafanaDashboardGroup    = "integreatly.org"
	grafanaDashboardResource = "grafanadashboards"
	grafanaDashboardVersion  = "v1alpha1"

	certificateGroup    = "cert-manager.io"
	certificateResource = "certificates"
	certificateVersion  = "v1"
)

func (mc *migrationCleaner) DeleteESCServiceMonitors(ctx context.Context) error {
	log.Info(ctx, "Start removing service monitors...")

	gvr := schema.GroupVersionResource{
		Group:    serviceMonitorGroup,
		Resource: serviceMonitorResource,
		Version:  serviceMonitorVersion,
	}

	srList, err := mc.dynamicClient.Resource(gvr).
		Namespace(mc.namespace).
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil && errors.IsNotFound(err) {
		log.Info(ctx, "Resource %s does not exist in cluster, skip it", grafanaDashboardResource)
		return nil
	} else if err != nil {
		log.Error(ctx, err, "Error getting ESC service monitors")
		return err
	}

	log.Info(ctx, "Found %d ESC service monitors to remove", len(srList.Items))

	for _, item := range srList.Items {
		err := mc.dynamicClient.Resource(gvr).
			Namespace(mc.namespace).
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s/%s service monitor", mc.namespace, item.GetName())
			return err
		}
		log.Debug(ctx, "Service monitor %s/%s is removed", mc.namespace, item.GetName())
	}

	log.Info(ctx, "All service monitors are removed")
	return nil
}

func (mc *migrationCleaner) DeleteESCGrafanaDashboards(ctx context.Context) error {
	log.Info(ctx, "Start removing grafana dashboards...")

	gvr := schema.GroupVersionResource{
		Group:    grafanaDashboardGroup,
		Resource: grafanaDashboardResource,
		Version:  grafanaDashboardVersion,
	}

	gdList, err := mc.dynamicClient.Resource(gvr).
		Namespace(mc.namespace).
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil && errors.IsNotFound(err) {
		log.Info(ctx, "Resource %s does not exist in cluster, skip it", grafanaDashboardResource)
		return nil
	} else if err != nil {
		log.Error(ctx, err, "Error getting ESC grafana dashboards")
		return err
	}

	log.Info(ctx, "Found %d ESC grafana dashboards to remove", len(gdList.Items))

	for _, item := range gdList.Items {
		err := mc.dynamicClient.Resource(gvr).
			Namespace(mc.namespace).
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s/%s grafana dashboard", mc.namespace, item.GetName())
			return err
		}
		log.Debug(ctx, "Grafana dashboard %s/%s is removed", mc.namespace, item.GetName())
	}

	log.Info(ctx, "All gradana dashboards are removed")
	return nil
}

func (mc *migrationCleaner) DeleteESCCertificates(ctx context.Context) error {
	log.Info(ctx, "Start removing certificates...")

	gvr := schema.GroupVersionResource{
		Group:    certificateGroup,
		Resource: certificateResource,
		Version:  certificateVersion,
	}

	certList, err := mc.dynamicClient.Resource(gvr).
		Namespace(mc.namespace).
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil && errors.IsNotFound(err) {
		log.Info(ctx, "Resource %s does not exist in cluster, skip it", certificateResource)
		return nil
	} else if err != nil {
		log.Error(ctx, err, "Error getting ESC certificates")
		return err
	}

	log.Info(ctx, "Found %d ESC certificates to remove", len(certList.Items))

	for _, item := range certList.Items {
		err := mc.dynamicClient.Resource(gvr).
			Namespace(mc.namespace).
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s/%s certificate", mc.namespace, item.GetName())
			return err
		}
		log.Debug(ctx, "Certificate %s/%s is removed", mc.namespace, item.GetName())
	}

	log.Info(ctx, "All certificates are removed")
	return nil
}
