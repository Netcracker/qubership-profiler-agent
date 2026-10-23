package hotread_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotread"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// orderedCall is one planted call; bufferOffs makes its PK unique and
// orders ties.
type orderedCall struct {
	offsetMs                    int64
	duration, cpu, queue, calls int
	netRead, netWritten         int
	bufferOffs                  int
}

// TestInternalCallsEveryOrder pages /internal/v1/calls through every order
// of 02 §2.3.5 over two partitions, two rows at a time, and checks the pages
// concatenate into the order with ties broken by PK. The keys are small, so
// every order has ties, and a tie group straddles most page cuts.
func TestInternalCallsEveryOrder(t *testing.T) {
	store := openTestStore(t)
	pr := addPod(t, store, "pod-1", 1, "com.example.M")
	var calls []orderedCall
	for i := range 9 {
		c := orderedCall{
			offsetMs: int64(i%3)*6*60_000 + int64(i), // three partitions
			duration: 100 * (i % 4), cpu: i % 3, queue: (i * 2) % 5, calls: i % 2,
			netRead: i % 4, netWritten: (i * 3) % 4, bufferOffs: i * 10,
		}
		calls = append(calls, c)
		addCall(t, pr, baseMs+c.offsetMs, data.Call{
			Method: 0, Duration: data.LDuration(c.duration), CpuTime: data.LTime(c.cpu),
			QueueWaitDuration: data.LDuration(c.queue), Calls: data.LCounter(c.calls),
			NetRead: data.LBytes(c.netRead), NetWritten: data.LBytes(c.netWritten),
			ThreadName: "t", TraceFileIndex: 1, BufferOffset: c.bufferOffs,
		})
	}
	srv := httptest.NewServer(hotread.New(store).Handler())
	t.Cleanup(srv.Close)

	for _, order := range model.ImplementedOrders {
		key := func(c orderedCall) int64 {
			switch order.Field() {
			case model.OrderFieldDuration:
				return int64(c.duration)
			case model.OrderFieldCPU:
				return int64(c.cpu)
			case model.OrderFieldQueue:
				return int64(c.queue)
			case model.OrderFieldCalls:
				return int64(c.calls)
			case model.OrderFieldNet:
				return int64(c.netRead + c.netWritten)
			}
			return baseMs + c.offsetMs
		}
		want := append([]orderedCall(nil), calls...)
		sort.SliceStable(want, func(i, j int) bool {
			if ki, kj := key(want[i]), key(want[j]); ki != kj {
				return (ki > kj) == order.Desc()
			}
			return want[i].bufferOffs < want[j].bufferOffs
		})
		var wantOffs []int
		for _, c := range want {
			wantOffs = append(wantOffs, c.bufferOffs)
		}

		var got []int
		params := url.Values{"from": {fmt.Sprint(baseMs)}, "to": {fmt.Sprint(baseMs + 30*60_000)},
			"order": {order.String()}, "limit": {"2"}}
		for page := 0; page < 20; page++ {
			var resp struct {
				Calls []model.CallJSON `json:"calls"`
				Order string           `json:"order"`
			}
			require.Equal(t, http.StatusOK, getJSON(t, srv, "/internal/v1/calls", params, &resp), order)
			assert.Equal(t, order.String(), resp.Order)
			if len(resp.Calls) == 0 {
				break
			}
			for _, c := range resp.Calls {
				got = append(got, int(c.PK.BufferOffset))
			}
			last := resp.Calls[len(resp.Calls)-1].Row(model.TierHot).Position()
			params.Set(model.AfterKeyParam(order), fmt.Sprint(order.Key(last)))
			params.Set("after_pk", last.PK.PathString())
		}
		assert.Equal(t, wantOffs, got, order)
	}

	t.Run("a position with no PK passes only later keys", func(t *testing.T) {
		params := url.Values{"from": {fmt.Sprint(baseMs)}, "to": {fmt.Sprint(baseMs + 30*60_000)},
			"order": {"net_asc"}, "after_key": {"3"}}
		var resp struct {
			Calls []model.CallJSON `json:"calls"`
		}
		require.Equal(t, http.StatusOK, getJSON(t, srv, "/internal/v1/calls", params, &resp))
		require.NotEmpty(t, resp.Calls)
		for _, c := range resp.Calls {
			assert.Greater(t, c.NetRead+c.NetWritten, int64(3))
		}
	})
}
