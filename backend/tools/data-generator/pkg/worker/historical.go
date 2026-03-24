package worker

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/files"
	"github.com/Netcracker/qubership-profiler-backend/libs/parquet"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"
)

// ProcessHistorical provide historical data to storages
//   - Generate parquet files for historical calls
//   - Persist data to S3 and PG
func (cg *ToolWorker) ProcessHistorical(ctx context.Context) {
	startTime := time.Now()
	var err error
	if cg.cloud.Disabled() {
		log.Info(ctx, "Skip uploading to S3")
		return
	}
	log.Info(ctx, "Uploading to %v/%v", cg.cfg.S3.Endpoint, cg.cfg.S3.BucketName)

	var hoursRange = cg.cfg.Limit.Range
	var uploaded, total, errors int
	hours := hoursRange.Hours()

	log.Info(ctx, "Preparing to upload %d hours in [%v] ", len(hours), hoursRange)
	for _, t := range hours {
		hourStartTime := time.Now()
		bucketDirName := common.DateHour(t)

		// Clear previous files
		err = cg.callsFm.ClearDirectory(ctx)
		if err != nil {
			log.Fatal(ctx, err, "Could not clear directory")
		}
		cg.callsFm = parquet.NewFileMap(cg.callsFm.CurrentDir, cg.callsFm.ParquetParams)
		cg.cloud.SetFileCache(cg.callsFm)

		// Generate new set for current hour
		err = cg.generateParquetFiles(ctx, t)
		if err != nil {
			log.Fatal(ctx, err, "Problem with calls generation")
		}

		// Persist generated parquet files + metadata
		var c1, t1, c2, t2 int
		c1, t1, err = cg.cloud.UploadDir(ctx, t, cg.cloud.GetCallsFiles())
		if err != nil {
			errors++
		} else {
			uploaded += c1
			total += t1
		}

		if err == nil {
			log.Info(ctx, "Uploaded %d files (%d total) for %s in %v ",
				c1+c2, t1+t2, bucketDirName, time.Since(hourStartTime))
		}
		if errors > 5 {
			log.Error(ctx, nil, "Too many errors: stop at %d [%v] instead of end [%v]",
				len(hours), t, hoursRange.EndDate)
			break
		}
	}

	if err == nil {
		log.Info(ctx, "Uploaded %d files (%d total) for %d hours in %v ",
			uploaded, total, len(hours), time.Since(startTime))
	}

	if errors > 0 {
		log.Error(ctx, nil, "Total errors during upload: %d [in %v]", errors, time.Since(startTime))
	}

	if err != nil {
		log.Fatal(ctx, err, "Problem with processing historical data")
	}
}

func (cg *ToolWorker) generateParquetFiles(ctx context.Context, callsStartTime time.Time) (err error) {
	if cg.cfg.Limit.Calls == 0 {
		log.Info(ctx, "Skip generating calls files")
		return nil
	}
	log.Info(ctx, "Start of calls (%s) generation for 1 hours for interval starting %s", cg.cfg.Limit.Cloud(), callsStartTime.Format(time.RFC3339))
	for ns := 0; ns < cg.cfg.Limit.NS; ns++ {
		for _, dr := range model.Durations.List {
			err = cg.writeCallsToParquet(ctx, cg.cfg.Namespace(ns), dr, callsStartTime)
		}
		if err != nil {
			break
		}
	}

	log.Info(ctx, "Calls for 60 min are generated successfully")
	return err
}

func (cg *ToolWorker) writeCallsToParquet(ctx context.Context, namespace string, dr model.DurationRange, callsStartTime time.Time) error {
	calls := cg.data.calls[dr.Pos]
	if len(calls) == 0 {
		return nil
	}

	var size int64
	startTime := time.Now()

	pqFile, err := cg.callsFm.GetCallsFile(ctx, namespace, &dr, callsStartTime)
	if err != nil {
		log.Error(ctx, err, "Get file error")
		return err
	}
	defer func(pqFile *parquet.FileWorker, ctx context.Context) {
		err := pqFile.Close(ctx)
		if err != nil {
			log.Error(ctx, err, "Close error")
		}
	}(pqFile, ctx)

	// write
	for _, call := range calls {
		if call.Namespace != namespace {
			continue
		}
		pqFile.S3FileInfo.Services.AddList([]string{call.ServiceName})
		callParquet := call.CallParquet
		callParquet.Time = common.RandomTime(callsStartTime).UnixMilli()
		if err = pqFile.Write(ctx, callParquet); err != nil {
			break
		}
	}
	if err == nil {
		err = pqFile.WriteStop(ctx)
	}
	if err == nil {
		size, err = files.FileSize(ctx, pqFile.LocalFilePath)
		log.Info(ctx, "Saved %d calls to '%s' [%d Kb] successfully in %v",
			len(calls), pqFile.FileName, size/1024, time.Since(startTime))
	} else {
		log.Error(ctx, err, "Error during saving in %v", time.Since(startTime))
	}
	return err
}
