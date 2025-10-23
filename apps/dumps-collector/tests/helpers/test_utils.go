package helpers

import (
	"context"
	"os"
	"path/filepath"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers"

	cp "github.com/otiai10/copy"
)

const (
	pgImage    = "postgres:15.3-alpine"
	pgUser     = "postgres"
	pgPassword = "postgres"
	pgDBName   = "test-db"
)

var (
	dataDir, _     = filepath.Abs("../resources/test-data")
	TestBaseDir, _ = filepath.Abs("../../output")
)

var PgContainer *helpers.PostgresContainer

func CreateDbClient(ctx context.Context) db.DumpDbClient {
	if PgContainer == nil {
		PgContainer = helpers.CreatePgContainerWithMonitoring(ctx, time.Now(), true)
	}
	if PgContainer != nil {
		port, _ := PgContainer.MappedPort(ctx, "5432")
		host, _ := PgContainer.Host(ctx)

		params := db.DBParams{
			DBHost:     host,
			DBPort:     port.Int(),
			DBUser:     pgUser,
			DBPassword: pgPassword,
			DBName:     pgDBName,
		}

		client, err := db.NewDumpDbClient(ctx, params)

		if err != nil {
			log.Fatal(ctx, err, "invalid connection parameters: %s")
			return nil
		}
		return client
	}
	return nil
}

func StopTestDb(ctx context.Context) {
	if PgContainer == nil {
		log.Warning(ctx, "Postgres container is not initialized or already stopped.")
		return
	}

	// Terminate the Postgres container gracefully
	err := PgContainer.Terminate(ctx)
	if err != nil {
		log.Fatal(ctx, err, "error stopping and removing postgres container")
	} else {
		log.Info(ctx, "Postgres container stopped successfully.")
	}

	PgContainer = nil
}

func CopyPVDataToTestDir(ctx context.Context) {
	if err := cp.Copy(dataDir, TestBaseDir); err != nil {
		log.Fatal(ctx, err, "error copying test date directory from %s to %s", dataDir, TestBaseDir)
	}
}

func RemoveTestDir(ctx context.Context) {
	if err := os.RemoveAll(TestBaseDir); err != nil {
		log.Fatal(ctx, err, "error removing test date directory %s", TestBaseDir)
	}
}
