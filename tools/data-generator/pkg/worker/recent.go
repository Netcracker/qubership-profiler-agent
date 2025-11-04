package worker

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/tools/data-generator/pkg/data"
	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

// ProcessTemporary generate & populate data to temporary tables in PG
func (cg *ToolWorker) ProcessTemporary(ctx context.Context) {
	tempTables := prepareDbTables(ctx, cg.cfg, cg.postgres)

	if !cg.cfg.Limit.HasHourTime() {
		return
	}

	if !cg.cfg.Limit.IsHoursValid() {
		log.Error(ctx, nil, "Invalid time period (%v hours) for recent data", cg.cfg.Limit.HoursCount)
		return
	}

	err := cg.generateRecent(ctx)
	if err != nil {
		log.Fatal(ctx, err, "Problem with recent data generation")
	}

	// mark tables as ready after generation
	for _, table := range tempTables {
		table.Status = model.TableStatusReady
		if err := cg.postgres.UpdateTempTableInventory(ctx, *table); err != nil {
			log.Fatal(ctx, err, "Could not update status in temp table %s", table.TableName)
		}
	}
}

func (cg *ToolWorker) generateRecent(ctx context.Context) (err error) {
	startTime := time.Now()
	log.Info(ctx, "Start generating recent data for %s", cg.cfg.Limit.Cloud())

	err = cg.persistMeta(ctx)

	if err == nil {
		err = cg.persistCalls(ctx)
	}

	if err == nil {
		log.Info(ctx, "Recent data were generated successfully in %v", time.Since(startTime))
	}
	return err
}

func (cg *ToolWorker) persistMeta(ctx context.Context) (err error) {
	startTime := time.Now()
	log.Info(ctx, "Persisting meta data...")
	c := 0
	for _, pods := range cg.data.pods {
		for _, pod := range pods {
			if err := cg.postgres.InsertPod(ctx, pod.GetPodInfo()); err != nil {
				break
			}
			if err := cg.postgres.InsertPodRestart(ctx, pod.GetPodRestart()); err != nil {
				break
			}
			for _, param := range pod.GetParams() {
				if err := cg.postgres.InsertParam(ctx, param); err != nil {
					break
				}
			}
			for _, dict := range pod.GetDictionary() {
				if err := cg.postgres.InsertDictionary(ctx, dict); err != nil {
					break
				}
			}
			c++
		}
		if err != nil {
			break
		}
	}
	if err == nil {
		log.Info(ctx, "Persisted metadata for %d pods in %v", c, time.Since(startTime))
	}
	return err
}

func (cg *ToolWorker) persistCalls(ctx context.Context) (err error) {
	startTime := time.Now()
	calls, traces, badTraces := 0, 0, 0
	for _, hour := range cg.cfg.Limit.RecentHours() {
		for _, list := range cg.data.calls {
			for _, call := range list {
				tt := common.RandomTime(hour)
				err = cg.postgres.InsertCall(ctx, call.GetPGCall(tt))
				if err != nil {
					break
				}
				calls++
				e := cg.postgres.InsertTrace(ctx, tt, call.GetPGTrace())
				if e != nil {
					badTraces++ // skip possible duplicates...
				} else {
					traces++
				}
			}
		}
	}
	if err == nil {
		log.Info(ctx, "Persist %d calls and %d traces [%d duplicates] in %v",
			calls, traces, badTraces, time.Since(startTime))
	}
	return err
}

func prepareDbTables(ctx context.Context, cfg data.Config, db pg.DbClient) []*inventory.TempTableInfo {
	if db == nil {
		return []*inventory.TempTableInfo{}
	}
	err := db.InitSchema(ctx)
	if err != nil {
		log.Fatal(ctx, err, "Could not update pg schema")
	}

	tempTables, err := db.GetTempTablesNames(ctx)
	if err != nil {
		log.Fatal(ctx, err, "Could not truncate old data in tables")
	}

	err = db.DropTables(ctx, tempTables...)
	if err != nil {
		log.Fatal(ctx, err, "Could not delete %d temp tables", len(tempTables))
	}

	err = db.TruncateCDTTables(ctx)
	if err != nil {
		log.Fatal(ctx, err, "Could not truncate old data in tables")
	}

	if err != nil {
		log.Fatal(ctx, err, "Could not truncate old data in tables")
	}

	if cfg.Limit.HasHourTime() {
		tables := make([]*inventory.TempTableInfo, 0)
		for _, t := range cfg.Limit.RecentMinutes() {
			createdTables, err := db.CreateTempTables(ctx, t.Truncate(5*time.Minute))
			if err != nil {
				log.Fatal(ctx, err, "Could not create temp tables")
			}
			tables = append(tables, createdTables...)
		}
		return tables
	}
	return []*inventory.TempTableInfo{}
}
