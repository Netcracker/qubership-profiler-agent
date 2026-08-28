// Command ui-seed feeds a running dev stack (backend/docker-compose.yaml)
// with synthetic agent traffic so the embedded UI has something to show —
// the data source of the it-e2e query-ui suite (07-ui-design.md §7). It
// emulates a few pod agents over the real TCP protocol, waits until the
// query service serves the rows, and exits.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	profio "github.com/Netcracker/qubership-profiler-backend/libs/io"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/pkg/errors"
)

const (
	mAPI = iota // void com.example.shop.Api.handle(...)
	mCart
	mDb
	tagRequestID
	tagSQL
)

var dictWords = []string{
	"void com.example.shop.Api.handle(HttpRequest) (Api.java:20) [BOOT-INF/lib/shop.jar]",
	"boolean com.example.shop.CartService.validate(Cart) (CartService.java:44) [BOOT-INF/lib/shop.jar]",
	"ResultSet com.example.shop.OrderDao.query(String) (OrderDao.java:71) [BOOT-INF/lib/shop-dao.jar]",
	"request.id",
	"sql",
}

// A duration mix that covers every retention class and both sides of the
// UI's default >500ms chip.
var durationsMs = []int{45, 120, 650, 900, 1800, 90, 5200, 300, 750, 60}

type noopListener struct{}

func (noopListener) Command(model.Command, time.Duration, error) {}
func (noopListener) Read(int, time.Duration, error)              {}
func (noopListener) Write(int, time.Duration, error)             {}
func (noopListener) Error(error)                                 {}
func (noopListener) IsAlive() (bool, error)                      { return true, nil }

func main() {
	agentAddr := flag.String("agent", "localhost:1715", "collector agent TCP address")
	queryURL := flag.String("query", "http://localhost:8080", "query service base URL")
	namespace := flag.String("namespace", "e2e", "pod namespace")
	service := flag.String("service", "shop", "pod service")
	pods := flag.Int("pods", 2, "number of emulated pods")
	calls := flag.Int("calls", 30, "calls per pod")
	flag.Parse()

	ctx := context.Background()
	for i := 0; i < *pods; i++ {
		pod := fmt.Sprintf("%s-%d", *service, i+1)
		if err := seedPod(ctx, *agentAddr, *namespace, *service, pod, *calls); err != nil {
			fmt.Fprintf(os.Stderr, "seed %s: %s\n", pod, err)
			os.Exit(1)
		}
		fmt.Printf("seeded %s/%s/%s with %d calls\n", *namespace, *service, pod, *calls)
	}
	if err := waitVisible(*queryURL, *namespace, *service, fmt.Sprintf("%s-1", *service), *calls); err != nil {
		fmt.Fprintf(os.Stderr, "calls did not become visible: %s\n", err)
		os.Exit(1)
	}
	fmt.Println("query serves the seeded calls; the UI is ready to browse")
}

func seedPod(ctx context.Context, agentAddr, namespace, service, pod string, calls int) error {
	// Recent enough to stay in the hot tier for the whole e2e run.
	baseMs := time.Now().Add(-2 * time.Minute).UnixMilli()

	chunks := make([]wire.TraceChunk, 0, calls)
	records := make([]wire.CallRecord, 0, calls)
	const spacingMs = 1500
	for j := 0; j < calls; j++ {
		durationMs := durationsMs[j%len(durationsMs)]
		requestID := fmt.Sprintf("req-%s-%04d", pod, j)
		sql := "select o.id, o.total from orders o where o.customer_id = ? and o.status = ?"
		if j%3 == 0 {
			sql = "update inventory set reserved = reserved + ? where sku = ?"
		}
		chunks = append(chunks, wire.TraceChunk{
			ThreadId: uint64(10 + j%7),
			StartMs:  baseMs + int64(j*spacingMs),
			Events: []wire.TraceEvent{
				wire.Enter(0, mAPI),
				wire.Tag(1, tagRequestID, requestID),
				wire.Enter(2, mCart),
				wire.Enter(3, mDb),
				wire.Tag(4, tagSQL, sql),
				wire.Exit(max(4, durationMs*7/10)),
				wire.Exit(max(5, durationMs*8/10)),
				wire.Exit(durationMs),
			},
		})
		delta := int64(spacingMs)
		if j == 0 {
			delta = 0
		}
		records = append(records, wire.CallRecord{
			DeltaMs:        delta,
			Method:         mAPI,
			DurationMs:     durationMs,
			ChildCalls:     2 + j%9,
			ThreadName:     fmt.Sprintf("http-nio-8080-exec-%d", 1+j%7),
			TraceFileIndex: 1,
			Params:         map[int][]string{tagRequestID: {requestID}, tagSQL: {"1"}},
			LogsGenerated:  int64(200 + j*7),
			LogsWritten:    int64(100 + j*3),
			CpuTimeMs:      int64(durationMs * 6 / 10),
			WaitTimeMs:     int64(durationMs / 10),
			MemoryUsed:     int64(durationMs) * 2048,
			FileRead:       int64(j) * 512,
			FileWritten:    int64(j) * 128,
			NetRead:        int64(durationMs) * 100,
			NetWritten:     int64(durationMs) * 40,
			Transactions:   1 + j%3,
			QueueWaitMs:    j % 25,
		})
	}
	trace, offsets := wire.TraceStream(baseMs-1_000, chunks)
	for j := range records {
		records[j].BufferOffset = int(offsets[j])
		records[j].RecordIndex = 0
	}

	ac := emulator.PrepareAgent(ctx, nil, noopListener{}, pod)
	err := ac.Prepare(emulator.ConnectionOpts{
		ProtocolAddress: agentAddr,
		Timeout: profio.TcpTimeout{
			ConnectTimeout: 10 * time.Second,
			SessionTimeout: 60 * time.Second,
			ReadTimeout:    5 * time.Second,
			WriteTimeout:   5 * time.Second,
		},
	}).Connect()
	if err != nil {
		return errors.Wrap(err, "connect")
	}
	if err := ac.InitializeConnection(model.PROTOCOL_VERSION_V3, namespace, service, pod); err != nil {
		return errors.Wrap(err, "initialize")
	}

	for _, stream := range []struct {
		name string
		data []byte
	}{
		{model.StreamDictionary, wire.DictionaryStream(dictWords)},
		{model.StreamTrace, trace},
		{model.StreamCalls, wire.CallsStreamRecords(baseMs, records)},
		{model.StreamSuspend, wire.SuspendStream(baseMs, []wire.SuspendEvent{{DeltaMs: 40, AmountMs: 5}})},
	} {
		handle, err := ac.CommandInitStream(stream.name, 0, false)
		if err != nil {
			return errors.Wrapf(err, "init stream %s", stream.name)
		}
		for pos := 0; pos < len(stream.data); pos += emulator.MaxBufSize {
			end := min(pos+emulator.MaxBufSize, len(stream.data))
			if err := ac.CommandRcvData(stream.name, handle, stream.data[pos:end]); err != nil {
				return errors.Wrapf(err, "send stream %s", stream.name)
			}
		}
		if err := ac.Flush(); err != nil {
			return errors.Wrap(err, "flush")
		}
		if err := ac.WaitForAcks(); err != nil {
			return errors.Wrap(err, "acks")
		}
	}
	if err := ac.CommandClose(); err != nil {
		return errors.Wrap(err, "close")
	}
	return ac.Close()
}

func waitVisible(queryURL, namespace, service, pod string, want int) error {
	deadline := time.Now().Add(90 * time.Second)
	q := url.Values{}
	q.Set("from", fmt.Sprint(time.Now().Add(-15*time.Minute).UnixMilli()))
	q.Set("to", fmt.Sprint(time.Now().Add(time.Minute).UnixMilli()))
	q.Set("pod", namespace+"/"+service+"/"+pod)
	q.Set("limit", "1000")
	for time.Now().Before(deadline) {
		resp, err := http.Get(queryURL + "/api/v1/calls?" + q.Encode())
		if err == nil {
			var page struct {
				Calls []json.RawMessage `json:"calls"`
			}
			decodeErr := json.NewDecoder(resp.Body).Decode(&page)
			_ = resp.Body.Close()
			if decodeErr == nil && resp.StatusCode == http.StatusOK && len(page.Calls) >= want {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return errors.Errorf("%s/%s/%s never reached %d visible calls", namespace, service, pod, want)
}
