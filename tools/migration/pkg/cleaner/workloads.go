package cleaner

import (
	"context"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (mc *migrationCleaner) DeleteESCDeployments(ctx context.Context) error {
	log.Info(ctx, "Start removing deployments...")

	deploymentsList, err := mc.kubeClientSet.AppsV1().
		Deployments(mc.namespace).
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil {
		log.Error(ctx, err, "Error getting ESC deployments")
		return err
	}

	log.Info(ctx, "Found %d ESC deployments to remove", len(deploymentsList.Items))

	for _, item := range deploymentsList.Items {
		err := mc.kubeClientSet.AppsV1().
			Deployments(mc.namespace).
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s/%s deployment", mc.namespace, item.GetName())
			return err
		}
		log.Debug(ctx, "Deployment %s/%s is removed", mc.namespace, item.GetName())
	}

	log.Info(ctx, "All deployments are removed")
	return nil
}
