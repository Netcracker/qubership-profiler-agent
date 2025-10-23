package cleaner

import (
	"context"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (mc *migrationCleaner) DeleteESCClusterRoleBindings(ctx context.Context) error {
	log.Info(ctx, "Start removing cluster role bindings...")

	crbList, err := mc.kubeClientSet.RbacV1().
		ClusterRoleBindings().
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil {
		log.Error(ctx, err, "Error getting ESC cluster role bindings")
		return err
	}

	log.Info(ctx, "Found %d ESC cluster role bindings to remove", len(crbList.Items))

	for _, item := range crbList.Items {
		err := mc.kubeClientSet.RbacV1().
			ClusterRoleBindings().
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s cluster role binding", item.GetName())
			return err
		}
		log.Debug(ctx, "Cluster role binding %s is removed", item.GetName())
	}

	log.Info(ctx, "All cluster role bindings are removed")
	return nil
}

func (mc *migrationCleaner) DeleteESCClusterRoles(ctx context.Context) error {
	log.Info(ctx, "Start removing cluster roles...")

	crList, err := mc.kubeClientSet.RbacV1().
		ClusterRoles().
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil {
		log.Error(ctx, err, "Error getting ESC cluster roles")
		return err
	}

	log.Info(ctx, "Found %d ESC cluster roles to remove", len(crList.Items))

	for _, item := range crList.Items {
		err := mc.kubeClientSet.RbacV1().
			ClusterRoles().
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s cluster roles", item.GetName())
			return err
		}
		log.Debug(ctx, "Cluster roles %s is removed", item.GetName())
	}

	log.Info(ctx, "All cluster roles are removed")
	return nil
}

func (mc *migrationCleaner) DeleteESCRoleBindings(ctx context.Context) error {
	log.Info(ctx, "Start removing role bindings...")

	rbList, err := mc.kubeClientSet.RbacV1().
		RoleBindings(mc.namespace).
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil {
		log.Error(ctx, err, "Error getting ESC role bindings")
		return err
	}

	log.Info(ctx, "Found %d ESC role bindings to remove", len(rbList.Items))

	for _, item := range rbList.Items {
		err := mc.kubeClientSet.RbacV1().
			RoleBindings(mc.namespace).
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s/%s role binding", mc.namespace, item.GetName())
			return err
		}
		log.Debug(ctx, "Role binding %s/%s is removed", mc.namespace, item.GetName())
	}

	log.Info(ctx, "All role bindings are removed")
	return nil
}

func (mc *migrationCleaner) DeleteESCRoles(ctx context.Context) error {
	log.Info(ctx, "Start removing roles...")

	rolesList, err := mc.kubeClientSet.RbacV1().
		Roles(mc.namespace).
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil {
		log.Error(ctx, err, "Error getting ESC roles")
		return err
	}

	log.Info(ctx, "Found %d ESC roles to remove", len(rolesList.Items))

	for _, item := range rolesList.Items {
		err := mc.kubeClientSet.RbacV1().
			Roles(mc.namespace).
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s/%s roles", mc.namespace, item.GetName())
			return err
		}
		log.Debug(ctx, "Roles %s/%s is removed", mc.namespace, item.GetName())
	}

	log.Info(ctx, "All roles are removed")
	return nil
}

func (mc *migrationCleaner) DeleteESCServiceAccounts(ctx context.Context) error {
	log.Info(ctx, "Start removing service accounts...")

	saList, err := mc.kubeClientSet.CoreV1().
		ServiceAccounts(mc.namespace).
		List(ctx, v1.ListOptions{
			LabelSelector: mc.labelSelector,
		})
	if err != nil {
		log.Error(ctx, err, "Error getting ESC service accounts")
		return err
	}

	log.Info(ctx, "Found %d ESC service accounts to remove", len(saList.Items))

	for _, item := range saList.Items {
		err := mc.kubeClientSet.CoreV1().
			ServiceAccounts(mc.namespace).
			Delete(ctx, item.GetName(), v1.DeleteOptions{})
		if err != nil {
			log.Error(ctx, err, "Error removing %s/%s service accounts", mc.namespace, item.GetName())
			return err
		}
		log.Debug(ctx, "Service account %s/%s is removed", mc.namespace, item.GetName())
	}

	log.Info(ctx, "All service accounts are removed")
	return nil
}
