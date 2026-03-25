package maintenance

import (
	"context"
	"path"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"
	"github.com/minio/minio-go/v7"
)

type filesPerDir struct {
	files map[string][]string
}

func (ftr *filesPerDir) AddFile(fileRemovePath string) {
	removePathDir := path.Dir(fileRemovePath)
	if _, ok := ftr.files[removePathDir]; !ok {
		ftr.files[removePathDir] = []string{}
	}
	ftr.files[removePathDir] = append(ftr.files[removePathDir], fileRemovePath)
}

type S3FileRemoveJob struct {
	*MaintenanceJob
	callsTs map[model.DurationRange]time.Time
	dumpsTs map[model.DumpType]time.Time
	heapsTs time.Time
}

func NewS3FileRemoveJob(ctx context.Context, mJob *MaintenanceJob, ts time.Time) (*S3FileRemoveJob, error) {
	callsTs := map[model.DurationRange]time.Time{}
	conf := &mJob.JobConfig.S3FileRemoval
	for _, dr := range conf.Calls.DurationRangesList() {
		callsTs[dr] = ts.Add(-time.Duration(conf.Calls.Get(dr)) * time.Hour)
	}
	dumpsTs := map[model.DumpType]time.Time{}
	for _, dt := range conf.Dumps.DumpTypesList() {
		dumpsTs[dt] = ts.Add(-time.Duration(conf.Dumps.Get(dt)) * time.Hour)
	}
	heapsTs := ts.Add(-time.Duration(conf.Heaps) * time.Hour)

	log.Debug(ctx, "Create new S3FileRemoveJob. Compaction time for durations: %v, compaction time for dump types: %v, compaction time for heap types: %v", callsTs, dumpsTs, heapsTs)

	return &S3FileRemoveJob{
		MaintenanceJob: mJob,
		callsTs:        callsTs,
		dumpsTs:        dumpsTs,
		heapsTs:        heapsTs,
	}, nil
}

func (frj *S3FileRemoveJob) Execute(ctx context.Context) error {
	startTime := time.Now()

	// Get files, that should be removed
	s3FilesToRemove, err := frj.getS3FilesToRemove(ctx)
	if err != nil {
		log.Error(ctx, err, "Error calculating s3 files that should be removed")
		return err
	}

	// Update the status for s3 files and collect the remote paths to delete
	filesPerDir := filesPerDir{files: map[string][]string{}}
	for _, file := range s3FilesToRemove {
		file.Status = model.FileDeleted
		if err := frj.Postgres.UpdateS3File(ctx, *file); err != nil {
			log.Error(ctx, err, "error updating the status for s3 file %s", file)
		} else {
			filesPerDir.AddFile(file.RemoteStoragePath)
		}
	}

	// Drop s3 files per directory
	successfulFiles := 0
	for remotePathDir, remotePaths := range filesPerDir.files {
		log.Debug(ctx, "Analizing %s directory in remote storage...", remotePathDir)
		// Get the list of files for directory
		objList, err := frj.MinioClient.ListObjectsWithPrefix(ctx, remotePathDir)
		if err != nil {
			log.Error(ctx, err, "error getting the list of files in s3 storage with prefix %s", remotePathDir)
		}
		// Filter the files, that should be removed
		var objListToRemove = make([]*minio.ObjectInfo, 0, len(objList)/4) // Normally only one file type per dir should be removed
		for _, obj := range objList {
			if _, ok := s3FilesToRemove[obj.Key]; ok {
				objListToRemove = append(objListToRemove, obj)
			}
		}
		// Remove filtered files
		log.Debug(ctx, "Going to remove %d files from %s directory", len(objListToRemove), remotePathDir)
		errs := frj.MinioClient.RemoveObjects(ctx, objListToRemove)
		for objName, err := range errs {
			log.Error(ctx, err, "error removing file %s from s3 storage", objName)
		}
		// Remove successful files from s3_files table
		for _, remotePath := range remotePaths {
			if _, ok := errs[remotePath]; !ok {
				s3File := s3FilesToRemove[remotePath]
				log.Debug(ctx, "Start removing file row %s", remotePath)
				if err := frj.Postgres.RemoveS3File(ctx, s3File.Uuid); err != nil {
					log.Error(ctx, err, "error removing s3 file inventory row %s", remotePath)
				} else {
					successfulFiles++
				}
			}
		}
	}

	log.Info(ctx, "S3FileRemoveJob is finished. Removed %d files. [Execution time - %v]", successfulFiles, time.Since(startTime))
	return nil
}

// getS3FilesToRemove create the list of s3files, that should be removed
func (frj *S3FileRemoveJob) getS3FilesToRemove(ctx context.Context) (map[string]*inventory.S3FileInfo, error) {
	var s3FilesToRemove = make(map[string]*inventory.S3FileInfo)

	for dr, ts := range frj.callsTs {
		// Get already existed s3 files from specified time range
		existS3Files, err := frj.Postgres.GetCallsS3FilesByDurationRangeAndStartTimeBetween(ctx, dr, time.Time{}, ts)
		if err != nil {
			return nil, err
		}

		// Check the status of exist s3 files and calculate the list of unexist ones
		for _, s3File := range existS3Files {
			if frj.checkS3FilesWithStatus(ctx, s3File) {
				s3FilesToRemove[s3File.RemoteStoragePath] = s3File
			}
		}
	}

	for dt, ts := range frj.dumpsTs {
		// Get already existed s3 files from specified time range
		existS3Files, err := frj.Postgres.GetDumpsS3FilesByTypeAndStartTimeBetween(ctx, dt, time.Time{}, ts)
		if err != nil {
			return nil, err
		}

		// Check the status of exist s3 files and calculate the list of unexist ones
		for _, s3File := range existS3Files {
			if frj.checkS3FilesWithStatus(ctx, s3File) {
				s3FilesToRemove[s3File.RemoteStoragePath] = s3File
			}
		}
	}

	existS3Files, err := frj.Postgres.GetHeapsS3FilesByStartTimeBetween(ctx, time.Time{}, frj.heapsTs)
	if err != nil {
		return nil, err
	}

	// Check the status of exist s3 files and calculate the list of unexist ones
	for _, s3File := range existS3Files {
		if frj.checkS3FilesWithStatus(ctx, s3File) {
			s3FilesToRemove[s3File.RemoteStoragePath] = s3File
		}
	}
	// Return the result
	return s3FilesToRemove, nil
}

// checkS3FilesWithStatus checks if specified s3 files exists in the db and warns, if it exists with not complited status (possible error)
func (frj *S3FileRemoveJob) checkS3FilesWithStatus(ctx context.Context, s3File *inventory.S3FileInfo) bool {
	// Warn, if s3File has not complited status
	// TODO: try to remove s3File with to_delete status?
	if s3File.Status != model.FileCompleted {
		log.Warning(ctx, "Found s3 file table with unexpected status: uuid = %s, file remote path = %s, file status = %s", s3File.Uuid, s3File.RemoteStoragePath, s3File.Status)
		return false
	}
	return true
}
