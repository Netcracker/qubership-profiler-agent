package s3

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// opCounts is one reading of the three cdt_minio_* series of one operation.
type opCounts struct {
	requests float64 // cdt_minio_operation_latency_seconds sample count
	objects  float64
	errors   float64
}

// readCounts reads the series of op from reg. The vecs are package globals
// shared by every test, so a test compares two readings rather than absolute
// values.
func readCounts(t *testing.T, reg *prometheus.Registry, op string) opCounts {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	var c opCounts
	seen := 0
	for _, f := range families {
		for _, m := range f.GetMetric() {
			if !hasLabel(m.GetLabel(), op) {
				continue
			}
			switch f.GetName() {
			case "cdt_minio_operation_latency_seconds":
				c.requests = float64(m.GetHistogram().GetSampleCount())
			case "cdt_minio_operation_objects_count":
				c.objects = m.GetCounter().GetValue()
			case "cdt_minio_operation_errors_count":
				c.errors = m.GetCounter().GetValue()
			default:
				continue
			}
			seen++
		}
	}
	require.Equal(t, 3, seen, "cdt_minio_* series found for %s=%q", OperationLabel, op)
	return c
}

func hasLabel(labels []*dto.LabelPair, op string) bool {
	for _, l := range labels {
		if l.GetName() == OperationLabel && l.GetValue() == op {
			return true
		}
	}
	return false
}

func newTestRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	RegisterMetrics(reg)
	return reg
}

func (c opCounts) minus(before opCounts) opCounts {
	return opCounts{c.requests - before.requests, c.objects - before.objects, c.errors - before.errors}
}

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func expiredContext() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()
	return ctx
}

func TestObserveResult(t *testing.T) {
	reg := newTestRegistry()
	cases := []struct {
		name string
		ctx  context.Context
		err  error
		want opCounts
	}{
		{"success counts the request and its objects", context.Background(), nil, opCounts{requests: 1, objects: 3}},
		{"an S3 failure counts one error", context.Background(), errors.New("503 slow down"), opCounts{errors: 1}},
		{"a cancelled caller records nothing", cancelledContext(),
			fmt.Errorf("get: %w", context.Canceled), opCounts{}},
		{"an expired caller deadline records nothing", expiredContext(),
			fmt.Errorf("get: %w", context.DeadlineExceeded), opCounts{}},
		{"a transport deadline under a live caller counts one error", context.Background(),
			fmt.Errorf("get: %w", context.DeadlineExceeded), opCounts{errors: 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := readCounts(t, reg, OperationGet)
			ObserveResult(c.ctx, OperationGet, time.Now(), 3, c.err)
			assert.Equal(t, c.want, readCounts(t, reg, OperationGet).minus(before))
		})
	}
}

// Each operation value lands on the series RegisterMetrics materialized
// under the spelling the dashboards and load-test specs query.
func TestObserveResult_LabelValues(t *testing.T) {
	reg := newTestRegistry()
	for _, op := range []string{"get", "list", "put", "remove", "remove_many"} {
		t.Run(op, func(t *testing.T) {
			before := readCounts(t, reg, op)
			ObserveResult(context.Background(), op, time.Now(), 1, nil)
			assert.Equal(t, opCounts{requests: 1, objects: 1}, readCounts(t, reg, op).minus(before))
		})
	}
	assert.ElementsMatch(t, []string{"get", "list", "put", "remove", "remove_many"},
		[]string{OperationGet, OperationList, OperationPut, OperationRemove, OperationRemoveMany})
}

func TestIsNotFound(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"NoSuchKey", minio.ErrorResponse{Code: "NoSuchKey", StatusCode: 404}, true},
		{"NoSuchBucket", minio.ErrorResponse{Code: "NoSuchBucket", StatusCode: 404}, false},
		{"a plain error", errors.New("connection reset"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, IsNotFound(c.err))
		})
	}
}
