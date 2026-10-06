package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/tools/migration/pkg/cleaner"
	"github.com/Netcracker/qubership-profiler-backend/tools/migration/pkg/envconfig"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type deleteDunction = func(ctx context.Context) error

func main() {
	// Initialize context and log
	ctx := context.Background()
	err := envconfig.InitConfig()
	if err != nil {
		fmt.Printf("Error during initializing env configuration: %s", err)
		os.Exit(1)
	}

	ctx, err = log.SetLevelString(ctx, envconfig.EnvConfig.LogLevel)
	if err != nil {
		fmt.Printf("Error during initializing logger: %s", err)
		os.Exit(1)
	}

	// Initialize migration cleaner
	migrationCleaner, err := cleaner.NewMigrationCleaner(ctx)
	if err != nil {
		os.Exit(1)
	}

	// Functions order
	deleteFunctionsOrder := []deleteDunction{
		migrationCleaner.DeleteESCCertificates,
		migrationCleaner.DeleteESCGrafanaDashboards,
		migrationCleaner.DeleteESCServiceMonitors,
		migrationCleaner.DeleteESCIngresses,
		migrationCleaner.DeleteESCServices,
		migrationCleaner.DeleteESCDeployments,
		migrationCleaner.DeleteESCSecrets,
		migrationCleaner.DeleteESCConfigMaps,
		migrationCleaner.DeleteESCRoleBindings,
		migrationCleaner.DeleteESCRoles,
	}

	if envconfig.EnvConfig.PrivilegedRights {
		deleteFunctionsOrder = append(deleteFunctionsOrder,
			migrationCleaner.DeleteESCClusterRoleBindings,
			migrationCleaner.DeleteESCClusterRoles,
		)
	}
	deleteFunctionsOrder = append(deleteFunctionsOrder, migrationCleaner.DeleteESCServiceAccounts)

	startTime := time.Now()
	for _, deleteFunction := range deleteFunctionsOrder {
		if err := deleteFunction(ctx); err != nil {
			os.Exit(1)
		}
	}
	log.Info(ctx, "All ESC objects are removed for %v", time.Since(startTime))
}
