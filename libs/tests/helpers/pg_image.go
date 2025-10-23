package helpers

import (
	"context"
	"path/filepath"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	pgImage    = "postgres:15.3-alpine"
	pgDatabase = "test-db"
	pgUser     = "postgres"
	pgPass     = "postgres"
)

var (
	SchemaFile = filepath.Join("..", "resources", "empty_schema.sql")
)

type PostgresContainer struct {
	*postgres.PostgresContainer
	Client     pg.DbClient
	Params     pg.Params
	tempTables []*inventory.TempTableInfo
}

func CreatePgContainer(ctx context.Context, ts time.Time) *PostgresContainer {
	return CreatePgContainerWithMonitoring(ctx, ts, false)
}

// CreatePgContainerWithMonitoring run a container (and stop tests if could not)
func CreatePgContainerWithMonitoring(ctx context.Context, ts time.Time, skipMonitoring bool) *PostgresContainer {
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage(pgImage),
		postgres.WithInitScripts(SchemaFile),
		postgres.WithDatabase(pgDatabase),
		postgres.WithUsername(pgUser),
		postgres.WithPassword(pgPass),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second),
		),
	)
	if err != nil {
		log.Fatal(ctx, err, "couldn't start pg container (%s)", pgImage)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatal(ctx, err, "invalid connection string: %s", connStr)
	}
	log.Debug(ctx, "Connected to %s", connStr)

	var params pg.Params
	if skipMonitoring {
		params = pg.Params{
			ConnStr:        connStr,
			SkipMonitoring: true,
		}
	} else {
		params = pg.Params{ConnStr: connStr}
	}

	client, err := pg.NewClient(ctx, params)
	if err != nil {
		log.Fatal(ctx, err, "couldn't connect to pg")
	}

	err = client.InitSchema(ctx)
	if err != nil {
		log.Fatal(ctx, err, "couldn't initialize schema")
	}

	var tempTables []*inventory.TempTableInfo
	times := []time.Time{ts.Add(-5 * time.Minute), ts, ts.Add(5 * time.Minute)}
	for _, ts := range times {
		tables, e := client.CreateTempTables(ctx, ts)
		if e != nil {
			log.Fatal(ctx, e, "couldn't initialize schema")
		}
		tempTables = append(tempTables, tables...)
		for _, t := range tables {
			log.Debug(ctx, "created temp table '%s' [%v] for %v - %v", t.TableName, t.Status, t.StartTime, t.EndTime)
		}
	}

	return &PostgresContainer{
		PostgresContainer: pgContainer,
		Client:            client,
		Params:            params,
		tempTables:        tempTables,
	}
}

func (pc *PostgresContainer) CleanupTempTables(ctx context.Context) (err error) {
	for _, t := range pc.tempTables {
		table := t.TableName
		err = pc.Client.TruncateTables(ctx, table)
		if err != nil {
			log.Error(ctx, err, "couldn't delete table '%s'", table)
		}
	}
	return err
}

func (pc *PostgresContainer) Terminate(ctx context.Context) error {
	err := pc.CleanupTempTables(ctx)
	if err == nil {
		err = (pc.PostgresContainer).Terminate(ctx)
	}
	if err != nil {
		log.Error(ctx, err, "error terminating postgres container")
	}
	return err
}
