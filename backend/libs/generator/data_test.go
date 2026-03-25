package generator

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/parser"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestPod_PrepareDumpPath(t *testing.T) {
	ts := time.Date(2024, 5, 5, 8, 31, 30, 0, time.UTC)
	p := generateTestPod()

	t.Run("path", func(t *testing.T) {
		assert.Equal(t, "ns1/2024/05/05/08/31/30/pod1/20240505T083130.td.txt", p.PrepareDumpPath(ts, "td"))
		assert.Equal(t, "ns1/2024/05/05/08/31/30/pod1/20240505T083130.top.txt", p.PrepareDumpPath(ts, "top"))
	})

	t.Run("current", func(t *testing.T) {
		assert.True(t, strings.HasPrefix(p.PreparePath("top"), "ns1/"))
		assert.True(t, strings.Contains(p.PreparePath("top"), "/pod1/"))
		assert.True(t, strings.HasSuffix(p.PreparePath("top"), ".top.txt"))
	})
}

func generateTestPod() *Pod {
	restart := time.Date(2023, 3, 30, 10, 0, 0, 0, time.UTC)

	p := &Pod{
		Namespace: "ns1",
		Service:   "svc1",
		PodName:   "pod1",
		Restart:   restart,
		Dumps: PodDumps{
			&DumpFile{},
			&DumpFile{},
			&parser.ParsedPodDump{},
		},
	}
	return p
}
