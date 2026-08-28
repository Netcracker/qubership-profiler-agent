package helpers

import (
	"context"
	"os"
	"path/filepath"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client/sqlite"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	cp "github.com/otiai10/copy"
)

var (
	dataDir, _     = filepath.Abs("../resources/test-data")
	TestBaseDir, _ = filepath.Abs("../../output")
)

func CreateDbClient(ctx context.Context) db.DumpDbClient {
	params := db.DBParams{
		DBPath: "file::memory:?cache=shared",
	}

	client, err := sqlite.NewClient(ctx, params)
	if err != nil {
		log.Fatal(ctx, err, "error creating SQLite test client")
		return nil
	}
	return client
}

func StopTestDb(ctx context.Context) {
	// No-op for SQLite in-memory databases — they are cleaned up automatically
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
