package model

import (
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/stretchr/testify/assert"
)

func TestCallWithTraces_String(t *testing.T) {
	ts := time.Date(2024, 2, 21, 10, 2, 0, 0, time.UTC)
	t.Run("call", func(t *testing.T) {
		c := &CallWithTraces{
			Call: Call{
				Time: ts,
			},
			Trace: []byte("trace"),
		}
		assert.Equal(t, 410, len(c.String()))
	})
}

func TestCall_String(t *testing.T) {
	ts := time.Date(2024, 2, 21, 10, 2, 0, 0, time.UTC)
	t.Run("call", func(t *testing.T) {
		c := &Call{
			Time:    ts,
			CpuTime: 1023,
		}
		assert.Equal(t, 380, len(c.String()))
	})
}

func TestDump_String(t *testing.T) {
	uuid := common.ToUuid(common.UUID{1: 4})
	ts := time.Date(2024, 2, 21, 10, 2, 0, 0, time.UTC)
	t.Run("dump", func(t *testing.T) {
		d := &Dump{
			UUID:        uuid,
			CreatedTime: ts,
		}
		assert.Equal(t, 227, len(d.String()))
	})
}

func TestTrace_String(t *testing.T) {
	ts := time.Date(2024, 2, 21, 10, 2, 0, 0, time.UTC)
	t.Run("trace", func(t *testing.T) {
		tr := &Trace{
			PodName:     "pod",
			RestartTime: ts,
			Trace:       []byte("trace"),
		}
		assert.Equal(t, 127, len(tr.String()))
	})
}
