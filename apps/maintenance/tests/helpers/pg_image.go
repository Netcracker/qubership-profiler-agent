//go:build integration

package helpers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/prometheus/client_golang/prometheus"
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

type PostgresContainer struct {
	*postgres.PostgresContainer
	Client *pg.Client
	Params pg.Params
}

// CreatePgContainer run a container (and stop tests if could not)
func CreatePgContainer(ctx context.Context) *PostgresContainer {
	pgContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage(pgImage),
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

	params := pg.Params{ConnStr: connStr}

	client, err := pg.NewClient(ctx, params)
	if err != nil && !errors.As(err, &prometheus.AlreadyRegisteredError{}) {
		log.Fatal(ctx, err, "couldn't connect to pg")
	}

	err = client.InitSchema(ctx)
	if err != nil {
		log.Fatal(ctx, err, "couldn't initialize schema")
	}

	return &PostgresContainer{
		PostgresContainer: pgContainer,
		Client:            client,
		Params:            params,
	}
}

func (pc *PostgresContainer) AddPodsWithRestarts(ctx context.Context, ns string, svc string, started time.Time, lastActive time.Time) (*model.PodInfo, []*model.PodRestart, error) {
	podName := fmt.Sprintf("%s-00000-000", svc)
	podId := fmt.Sprintf("%s.%s", ns, podName)
	for ts := started; !ts.After(lastActive); ts = ts.Add(30 * time.Minute) {
		podRestart := model.PodRestart{
			PodId:       podId,
			Namespace:   ns,
			ServiceName: svc,
			PodName:     podName,
			RestartTime: ts,
			ActiveSince: ts,
			LastActive:  ts,
		}
		if err := pc.Client.InsertPodRestart(ctx, podRestart); err != nil {
			return nil, nil, err
		}
	}
	podRestarts, err := pc.Client.GetPodRestarts(ctx, ns, svc, podName)
	if err != nil {
		return nil, nil, err
	}
	pod := model.PodInfo{
		PodId:       podId,
		Namespace:   ns,
		ServiceName: svc,
		PodName:     podName,
		ActiveSince: started,
		LastRestart: lastActive,
		LastActive:  lastActive,
	}
	if err := pc.Client.InsertPod(ctx, pod); err != nil {
		return nil, nil, err
	}
	return &pod, podRestarts, nil
}

func (pc *PostgresContainer) AddTempTables(ctx context.Context, fromTs time.Time, toTs time.Time, tableStatus model.TableStatus) (map[string]*inventory.TempTableInfo, error) {
	for ts := fromTs; !ts.After(toTs); ts = ts.Add(5 * time.Minute) {
		tempTables, err := pc.Client.CreateTempTables(ctx, ts)
		if err != nil {
			return nil, err
		}

		for _, tempTable := range tempTables {
			tempTable.Status = tableStatus
			if err := pc.Client.UpdateTempTableInventory(ctx, *tempTable); err != nil {
				return nil, err
			}
		}
	}

	return pc.Client.GetTempTableByStartTimeBetween(ctx, fromTs, toTs)
}

func (pc *PostgresContainer) AddS3Files(ctx context.Context, fromTs time.Time, toTs time.Time, s3FileStatus model.FileStatus) (map[string]*inventory.S3FileInfo, error) {
	for ts := fromTs; !ts.After(toTs); ts = ts.Add(time.Hour) {
		for _, dr := range model.Durations.List {
			if err := pc.CreateCallsS3File(ctx, &dr, ts, s3FileStatus); err != nil {
				return nil, err
			}
		}
		for _, dumpType := range model.AllDumpTypes {
			if err := pc.CreateDumpsS3File(ctx, dumpType, ts, s3FileStatus); err != nil {
				return nil, err
			}
		}
		if err := pc.CreateHeapsS3File(ctx, ts, s3FileStatus); err != nil {
			return nil, err
		}
	}

	return pc.Client.GetS3FilesByStartTimeBetween(ctx, fromTs, toTs)
}

func (pc *PostgresContainer) CreateCallsS3File(ctx context.Context, dr *model.DurationRange, ts time.Time, status model.FileStatus) error {
	uuid := common.RandomUuid()
	callFileName := fmt.Sprintf("ns-0-%s.parquet", dr.Title)
	callFilePath := fmt.Sprintf("%s/%s", common.DateHour(ts), callFileName)
	s3File := inventory.PrepareCallsFileInfo(uuid, ts, ts, ts.Add(time.Hour), "ns-0", dr, callFileName, callFilePath)
	s3File.RemoteStoragePath = callFilePath
	s3File.Status = status
	return pc.Client.InsertS3File(ctx, *s3File)
}

func (pc *PostgresContainer) CreateDumpsS3File(ctx context.Context, dumpType model.DumpType, ts time.Time, status model.FileStatus) error {
	uuid := common.RandomUuid()
	dumpFileName := fmt.Sprintf("ns-0-%s.parquet", dumpType)
	dumpFilePath := fmt.Sprintf("%s/%s", common.DateHour(ts), dumpFileName)
	s3File := inventory.PrepareDumpsFileInfo(uuid, ts, ts, ts.Add(time.Hour), "ns-0", dumpType, dumpFileName, dumpFilePath)
	s3File.RemoteStoragePath = dumpFilePath
	s3File.Status = status
	return pc.Client.InsertS3File(ctx, *s3File)
}

// TODO: rework, when it was fully supported
func (pc *PostgresContainer) CreateHeapsS3File(ctx context.Context, ts time.Time, status model.FileStatus) error {
	uuid := common.RandomUuid()
	heapFileName := "heap.parquet"
	heapFilePath := fmt.Sprintf("%s/%s", common.DateHour(ts), heapFileName)
	s3File := inventory.PrepareDumpsFileInfo(uuid, ts, ts, ts.Add(time.Hour), "ns-0", model.DumpTypeHeap, heapFileName, heapFilePath)
	s3File.RemoteStoragePath = heapFilePath
	s3File.Type = model.FileHeap
	s3File.Status = status
	return pc.Client.InsertS3File(ctx, *s3File)
}

func (pc *PostgresContainer) CleanUpAllTempTables(ctx context.Context) error {
	tempTables, err := pc.Client.GetTempTablesNames(ctx)
	if err != nil {
		log.Error(ctx, err, "error getting temp tables")
		return err
	}
	if err = pc.Client.DropTables(ctx, tempTables...); err != nil {
		log.Error(ctx, err, "error removing temp tables")
		return err
	}
	if err = pc.Client.TruncateTables(ctx, pg.TempTableInventoryTable); err != nil {
		log.Error(ctx, err, "error truncating temp table inventory")
		return err
	}
	return nil
}

func (pc *PostgresContainer) CleanUpS3Files(ctx context.Context) error {
	if err := pc.Client.TruncateTables(ctx, pg.S3FilesTable); err != nil {
		log.Error(ctx, err, "error truncating s3 files inventory")
		return err
	}
	return nil
}

func (pc *PostgresContainer) CleanUpPods(ctx context.Context) error {
	if err := pc.Client.TruncateTables(ctx, pg.PodsTable); err != nil {
		log.Error(ctx, err, "error truncating pods")
		return err
	}
	if err := pc.Client.TruncateTables(ctx, pg.PodRestartsTable); err != nil {
		log.Error(ctx, err, "error truncating pods restarts")
		return err
	}
	return nil
}

func (pc *PostgresContainer) Terminate(ctx context.Context) error {
	err := (pc.PostgresContainer).Terminate(ctx)
	if err != nil {
		log.Error(ctx, err, "error terminating postgres container")
	}
	return err
}
