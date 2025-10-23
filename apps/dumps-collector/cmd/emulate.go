package cmd

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	model "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/server"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/task"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/google/uuid"
	"github.com/oklog/run"
	"github.com/spf13/cobra"
)

const (
	// pods count
	nsCount  = 2
	svcCount = 10
	podCount = 3

	// defaultDuration is duration between creating td/top dumps (summary should be minute)
	defaultDuration = time.Minute / (nsCount * svcCount * podCount)
	bindAddress     = "localhost:8080"

	// db params
	dbHost     = "localhost"
	dbPort     = 5432
	dbUser     = "postgres"
	dbPassword = "postgres"
	dbName     = "postgres"
)

// Time for data generation (14 days + 1 hour, that will be removed in first interation)
var (
	startTime = time.Date(2024, 06, 30, 23, 00, 00, 00, time.UTC)
	endTime   = time.Date(2024, 07, 15, 00, 00, 00, 00, time.UTC)

	startSearchTime = time.Date(2024, 07, 13, 00, 00, 00, 00, time.UTC)
	endSearchTime   = time.Date(2024, 07, 14, 00, 00, 00, 00, time.UTC)

	// heap dumps will be created every day for pods from filter
	heapDumpPodFilter = model.NewPodFilterComparator("pod_name", model.ComparatorEqual, "namespace-0-service-0-pod-0")
)

var emulateCmd = &cobra.Command{
	Use:   "emulate",
	Short: "Run emulator, that creates db and fills it with some dumps (without PV)",
	Run:   Emulate,
}

func init() {
	rootCmd.AddCommand(emulateCmd)
}

func Emulate(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	params := db.DBParams{
		DBHost:     dbHost,
		DBPort:     dbPort,
		DBUser:     dbUser,
		DBPassword: dbPassword,
		DBName:     dbName,
	}

	dbClient, err := db.NewDumpDbClient(ctx, params)
	if err != nil {
		log.Error(ctx, err, "cannot create new DB client")
		os.Exit(1)
	}

	requestProcessor, err := task.NewRequestProcessor("./tests/resources", dbClient, true)
	if err != nil {
		log.Fatal(ctx, err, "Error calculating request procesor")
	}

	podDist, heapDumpPods, err := GeneratePods(ctx, dbClient)
	if err != nil {
		log.Fatal(ctx, err, "Error generating pods")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var gr run.Group
	gr.Add(run.SignalHandler(ctx, os.Interrupt, syscall.SIGINT, syscall.SIGTERM))

	gr.Add(func() error {
		return server.StartHttpServer(ctx, requestProcessor, bindAddress)
	}, func(err error) {
		log.Error(ctx, err, "http server error")
		cancel()
	})
	gr.Add(func() error {
		if err := GenData(ctx, dbClient, podDist, heapDumpPods); err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	}, func(err error) {
		log.Error(ctx, err, "Found gen data error")
		cancel()
	})
	gr.Add(func() error {
		return InsertProcess(ctx, dbClient, podDist)
	}, func(err error) {
		log.Error(ctx, err, "Found inserting error")
		cancel()
	})
	gr.Add(func() error {
		return RemoveProcess(ctx, dbClient)
	}, func(err error) {
		log.Error(ctx, err, "Found removing error")
		cancel()
	})
	gr.Add(func() error {
		return PackProcess(ctx, dbClient)
	}, func(err error) {
		log.Error(ctx, err, "Found packing error")
		cancel()
	})
	gr.Add(func() error {
		return CalculateSummaryProcess(ctx, dbClient)
	}, func(err error) {
		log.Error(ctx, err, "Found calculate summary error")
		cancel()
	})

	if err := gr.Run(); err != nil {
		log.Info(ctx, "terminating... reason: %s", err)
	}
}

func GeneratePods(ctx context.Context, db db.DumpDbClient) (map[int][]model.Pod, []model.Pod, error) {
	// Pods distribution per second, is needed for insert process
	podsDist := map[int][]model.Pod{}

	t := startTime
	for nsi := 0; nsi < nsCount; nsi++ {
		ns := fmt.Sprintf("namespace-%d", nsi)
		for svci := 0; svci < svcCount; svci++ {
			svc := fmt.Sprintf("%s-service-%d", ns, svci)
			for pi := 0; pi < podCount; pi++ {
				podName := fmt.Sprintf("%s-pod-%d", svc, pi)
				pod, _, err := db.CreatePodIfNotExist(ctx, ns, svc, podName, startTime)
				if err != nil {
					return nil, nil, err
				}
				if podList, found := podsDist[t.Second()]; found {
					podsDist[t.Second()] = append(podList, *pod)
				} else {
					podsDist[t.Second()] = []model.Pod{*pod}
				}
				t = t.Add(defaultDuration)
			}
		}
	}

	heapDumpsPods, err := db.SearchPods(ctx, heapDumpPodFilter)
	if err != nil {
		return nil, nil, err
	}

	return podsDist, heapDumpsPods, nil
}

func GenData(ctx context.Context, dbc db.DumpDbClient, podsDist map[int][]model.Pod, heapDumpsPods []model.Pod) error {
	st := time.Now()

	// Generate per hours
	for hour := endTime; startTime.Before(hour); {
		curHour := hour.Add(-time.Hour)

		dumps := make([]model.DumpInfo, 0, nsCount*svcCount*podCount*120) //2 dumps every minute for one pod
		heapDumps := make([]model.DumpInfo, 0)
		for t := hour.Add(-time.Second); !t.Before(startTime) && !t.Before(curHour); t = t.Add(-time.Second) {
			pods := podsDist[t.Second()]
			for _, pod := range pods {
				dumps = append(dumps,
					model.DumpInfo{
						Pod:          pod,
						CreationTime: t.Truncate(time.Second),
						FileSize:     100,
						DumpType:     model.TdDumpType,
					},
					model.DumpInfo{
						Pod:          pod,
						CreationTime: t.Truncate(time.Second),
						FileSize:     50,
						DumpType:     model.TopDumpType,
					})
			}
		}

		if curHour.Hour() == 0 {
			for _, pod := range heapDumpsPods {
				heapDumps = append(heapDumps, model.DumpInfo{
					Pod:          pod,
					CreationTime: curHour,
					FileSize:     1000,
					DumpType:     model.HeapDumpType,
				})
			}
		}

		err := dbc.Transaction(ctx, func(tx db.DumpDbClient) error {
			if _, _, err := tx.CreateTimelineIfNotExist(ctx, curHour); err != nil {
				return err
			}
			if _, err := tx.InsertTdTopDumps(ctx, curHour, dumps); err != nil {
				return err
			}
			if _, err := tx.InsertHeapDumps(ctx, heapDumps); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}

		log.Info(ctx, "Added %d dumps for hour %v", len(dumps)+len(heapDumps), curHour)
		hour = curHour
	}
	log.Info(ctx, "Data generated for %v time", time.Since(st))
	return nil
}

func InsertProcess(ctx context.Context, dbc db.DumpDbClient, podsDist map[int][]model.Pod) error {
	t := endTime
	var duration time.Duration = 0
	count := 0

	pods := podsDist[t.Second()]
	dumpsInfo := make([]model.DumpInfo, 0, 2*len(pods))
	// Calculate dumps
	for _, pod := range pods {
		dumpsInfo = append(dumpsInfo, model.DumpInfo{
			Pod:          pod,
			FileSize:     100,
			CreationTime: t,
			DumpType:     model.TdDumpType,
		},

			model.DumpInfo{
				Pod:          pod,
				FileSize:     50,
				CreationTime: t,
				DumpType:     model.TopDumpType,
			})
	}

	for {
		select {
		case <-time.After(time.Second):
			st := time.Now()
			// Try to create test pods
			err := dbc.Transaction(ctx, func(tx db.DumpDbClient) error {
				for _, pod := range pods {
					if _, _, err := tx.CreatePodIfNotExist(ctx, pod.Namespace, pod.ServiceName, pod.PodName, pod.RestartTime); err != nil {
						return err
					}
				}

				// Try to create timeline
				if _, _, err := tx.CreateTimelineIfNotExist(ctx, t); err != nil {
					return err
				}

				// Insert dumps
				for _, dumpInfo := range dumpsInfo {
					if _, _, err := tx.CreateTdTopDumpIfNotExist(ctx, dumpInfo); err != nil {
						return err
					}
				}

				// Update last active for pods
				for _, pod := range pods {
					if _, err := tx.UpdatePodLastActive(ctx, pod.Namespace, pod.ServiceName, pod.PodName, pod.RestartTime, t); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return err
			}
			duration += time.Since(st)
			log.Info(ctx, "Insetred %d dumps, time %v", len(dumpsInfo), time.Since(st))
			t = t.Add(time.Second)
			count++
		case <-ctx.Done():
			log.Info(ctx, "Insert operation average time %v", time.Duration(int(duration)/count))
			return nil
		}
	}
}

func RemoveProcess(ctx context.Context, db db.DumpDbClient) error {
	t := startTime
	var duration time.Duration = 0
	count := 0

	for {
		select {
		case <-time.After(time.Hour):
			st := time.Now()

			// update timeline status
			if _, err := db.UpdateTimelineStatus(ctx, t, model.RemovingStatus); err != nil {
				return err
			}

			// remove files on PV

			// Delete old pods
			if _, err := db.RemoveOldPods(ctx, t.Add(time.Hour)); err != nil {
				return err
			}

			// delete timeline with table
			if _, err := db.RemoveTimeline(ctx, t); err != nil {
				return err
			}

			duration += time.Since(st)
			log.Info(ctx, "Removed timeline %v, time %v", t, time.Since(st))
			t = t.Add(time.Hour)
			count++
		case <-ctx.Done():
			log.Info(ctx, "Remove operation average time %v", time.Duration(int(duration)/count))
			return nil
		}
	}
}

func PackProcess(ctx context.Context, db db.DumpDbClient) error {
	t := endTime.Add(-time.Hour)
	var duration time.Duration = 0
	count := 0

	for {
		select {
		case <-time.After(time.Hour):
			st := time.Now()

			// update timeline status
			if _, err := db.UpdateTimelineStatus(ctx, t, model.ZippingStatus); err != nil {
				return err
			}

			// zip files files on PV

			// update timeline status
			if _, err := db.UpdateTimelineStatus(ctx, t, model.ZippedStatus); err != nil {
				return err
			}

			duration += time.Since(st)
			log.Info(ctx, "Packing timeline %v, time %v", t, time.Since(st))
			t = t.Add(time.Hour)
			count++
		case <-ctx.Done():
			log.Info(ctx, "Packing operation average time %v", time.Duration(int(duration)/count))
			return nil
		}
	}
}

func CalculateSummaryProcess(ctx context.Context, db db.DumpDbClient) error {
	var duration time.Duration = 0
	count := 0

	podFilter := model.NewPodFilterComparator("pod_name", model.ComparatorEqual, "namespace-0-service-0-pod-0")
	for {
		select {
		case <-time.After(time.Second * 10):
			st := time.Now()

			// search affected pods
			pods, err := db.SearchPods(ctx, podFilter)
			if err != nil {
				return err
			}
			podIds := make([]uuid.UUID, len(pods))
			for i, pod := range pods {
				podIds[i] = pod.Id
			}

			// search affected timelines
			timelines, err := db.SearchTimelines(ctx, startSearchTime, endSearchTime)
			if err != nil {
				return err
			}

			// search for any timeline
			summaries := make([]model.DumpSummary, 0, len(timelines))
			for _, timeline := range timelines {
				res, err := db.CalculateSummaryTdTopDumps(ctx, timeline.TsHour, podIds, startSearchTime, endSearchTime)
				if err != nil {
					return err
				}
				summaries = append(summaries, res...)
			}

			// Collect
			result := model.DumpSummary{}
			if len(summaries) > 0 {
				result = summaries[0]
			}

			for _, summary := range summaries {
				if result.DateFrom.After(summary.DateFrom) {
					result.DateFrom = summary.DateFrom
				}
				if result.DateTo.Before(summary.DateTo) {
					result.DateTo = summary.DateTo
				}
				result.SumFileSize += summary.SumFileSize
			}

			// search heap dumps

			duration += time.Since(st)
			log.Info(ctx, "Calculated statistics %+v, time %v", result, time.Since(st))
			count++
		case <-ctx.Done():
			log.Info(ctx, "Calculate operation average time %v", time.Duration(int(duration)/count))
			return nil
		}
	}
}
