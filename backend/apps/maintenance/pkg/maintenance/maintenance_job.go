package maintenance

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/index"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/pkg/config"
	"github.com/Netcracker/qubership-profiler-backend/libs/clock"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
)

type MaintenanceJob struct {
	Postgres            *pg.Client
	MinioClient         *s3.MinioClient
	JobConfig           *config.JobConfig
	InvertedIndexConfig *index.InvertedIndexConfig
}

// NewMaintenanceJob initializes a MaintenanceJob instance with configured Postgres and S3 clients.
// Returns an error if any of the required clients fail to initialize.
func NewMaintenanceJob(ctx context.Context, jobConfig *config.JobConfig, invertedIndexConfig *index.InvertedIndexConfig) (*MaintenanceJob, error) {
	postgres, err := pg.NewClient(ctx, *config.Cfg.Postgres)
	if err != nil {
		log.Error(ctx, err, "Failed to create Postgres client")
		return nil, err
	}

	minio, err := s3.NewClient(ctx, *config.Cfg.S3)
	if err != nil {
		log.Error(ctx, err, "Failed to create S3 client")
		return nil, err
	}

	return &MaintenanceJob{
		Postgres:            postgres,
		MinioClient:         minio,
		JobConfig:           jobConfig,
		InvertedIndexConfig: invertedIndexConfig,
	}, nil
}

// Execute runs the full maintenance pipeline.
// This includes:
//   - TempTablesCreationJob: creates temporary tables
//   - TempTablesRemoveJob: removes outdated temporary tables
//   - S3FileRemoveJob: cleans up S3 files
//   - MetadataRemoveJob: deletes stale metadata records
func (m *MaintenanceJob) Execute(ctx context.Context) error {
	ts := clock.Now().UTC()
	if err := m.ExecuteTempTablesCreation(ctx, ts); err != nil {
		log.Info(ctx, "TempTablesCreationJob failed, continuing")
	}
	if err := m.ExecuteTempTablesRemoving(ctx, ts); err != nil {
		log.Info(ctx, "TempTablesRemoveJob failed, continuing")
	}
	if err := m.ExecuteS3FilesRemoving(ctx, ts); err != nil {
		log.Info(ctx, "S3FileRemoveJob failed, continuing")
	}
	if err := m.ExecuteMetadataRemoving(ctx, ts); err != nil {
		log.Info(ctx, "MetadataRemoveJob failed, continuing")
	}
	return nil
}

// ExecuteTempTablesCreation initializes and executes TempTablesCreationJob for the provided timestamp.
// Returns an error if the job cannot be initialized or fails during execution.
func (m *MaintenanceJob) ExecuteTempTablesCreation(ctx context.Context, ts time.Time) error {
	job, err := NewTempTablesCreationJob(ctx, m, ts)
	if err != nil {
		log.Error(ctx, err, "Failed to initialize temp tables creation job")
		return err
	}
	if err = job.Execute(ctx); err != nil {
		log.Error(ctx, err, "Failed to execute temp tables creation job")
		return err
	}
	return nil
}

// ExecuteTempTablesRemoving initializes and executes TempTablesRemoveJob for the provided timestamp.
// Returns an error if the job cannot be initialized or fails during execution.
func (m *MaintenanceJob) ExecuteTempTablesRemoving(ctx context.Context, ts time.Time) error {
	job, err := NewTempTablesRemoveJob(ctx, m, ts)
	if err != nil {
		log.Error(ctx, err, "Failed to initialize temp tables remove job")
		return err
	}
	if err = job.Execute(ctx); err != nil {
		log.Error(ctx, err, "Failed to execute temp tables remove job")
		return err
	}
	return nil
}

// ExecuteS3FilesRemoving initializes and executes S3FileRemoveJob for the provided timestamp.
// Returns an error if the job cannot be initialized or fails during execution.
func (m *MaintenanceJob) ExecuteS3FilesRemoving(ctx context.Context, ts time.Time) error {
	job, err := NewS3FileRemoveJob(ctx, m, ts)
	if err != nil {
		log.Error(ctx, err, "Failed to initialize S3 files remove job")
		return err
	}
	if err = job.Execute(ctx); err != nil {
		log.Error(ctx, err, "Failed to execute S3 files remove job")
		return err
	}
	return nil
}

// ExecuteMetadataRemoving initializes and executes MetadataRemoveJob for the provided timestamp.
// Returns an error if the job cannot be initialized or fails during execution.
func (m *MaintenanceJob) ExecuteMetadataRemoving(ctx context.Context, ts time.Time) error {
	job, err := NewMetadataRemoveJob(ctx, m, ts)
	if err != nil {
		log.Error(ctx, err, "Failed to initialize metadata remove job")
		return err
	}
	if err = job.Execute(ctx); err != nil {
		log.Error(ctx, err, "Failed to execute metadata remove job")
		return err
	}
	return nil
}
