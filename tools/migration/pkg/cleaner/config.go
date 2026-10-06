package cleaner

import (
	"context"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (mc *migrationCleaner) DeleteESCConfigMaps(ctx context.Context) error {
	log.Info(ctx, "Start removing configmaps...")

	configMapsList, err := mc.kubeClientSet.CoreV1().
		ConfigMaps(mc.namespace).
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil {
		log.Error(ctx, err, "Error getting ESC configmaps")
		return err
	}

	log.Info(ctx, "Found %d ESC configmaps to remove", len(configMapsList.Items))

	for _, item := range configMapsList.Items {
		err := mc.kubeClientSet.CoreV1().
			ConfigMaps(mc.namespace).
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s/%s configmap", mc.namespace, item.GetName())
			return err
		}
		log.Debug(ctx, "Configmap %s/%s is removed", mc.namespace, item.GetName())
	}

	log.Info(ctx, "All configmaps are removed")
	return nil
}

func (mc *migrationCleaner) DeleteESCSecrets(ctx context.Context) error {
	log.Info(ctx, "Start removing secrets...")

	secretsList, err := mc.kubeClientSet.CoreV1().
		Secrets(mc.namespace).
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil {
		log.Error(ctx, err, "Error getting ESC secrets")
		return err
	}

	log.Info(ctx, "Found %d ESC secrets to remove", len(secretsList.Items))

	for _, item := range secretsList.Items {
		err := mc.kubeClientSet.CoreV1().
			Secrets(mc.namespace).
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s/%s secret", mc.namespace, item.GetName())
			return err
		}
		log.Debug(ctx, "Secret %s/%s is removed", mc.namespace, item.GetName())
	}

	log.Info(ctx, "All secrets are removed")
	return nil
}
