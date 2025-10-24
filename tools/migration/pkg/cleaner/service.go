package cleaner

import (
	"context"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (mc *migrationCleaner) DeleteESCIngresses(ctx context.Context) error {
	log.Info(ctx, "Start removing ingresses...")

	ingressesList, err := mc.kubeClientSet.NetworkingV1().
		Ingresses(mc.namespace).
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil {
		log.Error(ctx, err, "Error getting ESC ingresses")
		return err
	}

	log.Info(ctx, "Found %d ESC ingresses to remove", len(ingressesList.Items))

	for _, item := range ingressesList.Items {
		err := mc.kubeClientSet.NetworkingV1().
			Ingresses(mc.namespace).
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s/%s ingress", mc.namespace, item.GetName())
			return err
		}
		log.Debug(ctx, "Ingress %s/%s is removed", mc.namespace, item.GetName())
	}

	log.Info(ctx, "All ingresses are removed")
	return nil
}

func (mc *migrationCleaner) DeleteESCServices(ctx context.Context) error {
	log.Info(ctx, "Start removing services...")

	servicesList, err := mc.kubeClientSet.CoreV1().
		Services(mc.namespace).
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil {
		log.Error(ctx, err, "Error getting ESC services")
		return err
	}

	log.Info(ctx, "Found %d ESC services to remove", len(servicesList.Items))

	for _, item := range servicesList.Items {
		err := mc.kubeClientSet.CoreV1().
			Services(mc.namespace).
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s/%s service", mc.namespace, item.GetName())
			return err
		}
		log.Debug(ctx, "Service %s/%s is removed", mc.namespace, item.GetName())
	}

	log.Info(ctx, "All services are removed")
	return nil
}
