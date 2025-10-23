package generator

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

const (
	DataDir = "../tests/resources/data"
)

func createTestOptions() Options {
	opts := Options{
		DataDir:       DataDir,
		CollectorHost: "localhost",
		LogLevel:      "debug",
		Timeouts:      Timeouts{"2s", "10s"},
		Tags:          map[string]string{},
		TestDuration:  "4m",
		PodCount:      10,
		Prefixes:      Prefixes{"ns_", "svc_", "pod_"},
	}
	return opts
}

func TestOptions(t *testing.T) {
	opts := createTestOptions()

	t.Run("valid", func(t *testing.T) {
		assert.Nil(t, opts.Validate())
	})

	t.Run("invalid", func(t *testing.T) {
		t.Run("data dir", func(t *testing.T) {
			c := opts
			c.DataDir = ""
			assert.ErrorContains(t, c.Validate(), "empty path for data")
		})
		t.Run("session timeout", func(t *testing.T) {
			c := opts
			c.Timeouts.Session = ""
			assert.ErrorContains(t, c.Validate(), "empty session timeout")
			c.Timeouts.Session = "21sdfsdf"
			assert.ErrorContains(t, c.Validate(), "invalid session timeout: 21sdfsdf")
		})
		t.Run("connect timeout", func(t *testing.T) {
			c := opts
			c.Timeouts.Connect = ""
			assert.ErrorContains(t, c.Validate(), "empty connection timeout")
			c.Timeouts.Connect = "21sdfsdf"
			assert.ErrorContains(t, c.Validate(), "invalid connection timeout: 21sdfsdf")
		})
		t.Run("duration", func(t *testing.T) {
			c := opts
			c.TestDuration = ""
			assert.ErrorContains(t, c.Validate(), "empty test duration")
			c.TestDuration = "21sdfsdf"
			assert.ErrorContains(t, c.Validate(), "invalid test duration: 21sdfsdf")
		})
		t.Run("pods count", func(t *testing.T) {
			c := opts
			c.PodCount = 0
			assert.ErrorContains(t, c.Validate(), "invalid pod count: 0")
			c.PodCount = -100
			assert.ErrorContains(t, c.Validate(), "invalid pod count: -100")
		})
	})

	t.Run("options", func(t *testing.T) {
		assert.Equal(t, 2*time.Second, opts.ConnectTimeout())
		assert.Equal(t, 4*time.Minute, opts.Duration())
		assert.Equal(t, "localhost:1715", opts.ProtocolAddr())
		assert.Equal(t, 10*time.Second, opts.SessionTimeout())
	})
}

func TestPrefixes(t *testing.T) {
	t.Run("", func(t *testing.T) {
		p := Prefixes{
			Namespace: "test_ns",
			Service:   "test_ns",
			PodName:   "test_ns",
		}
		assert.Equal(t, "{ns:test_ns svc:test_ns pod:test_ns}", p.String())
	})
}
