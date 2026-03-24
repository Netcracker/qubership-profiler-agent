package parser

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
)

const (
	ResourceDir = "../tests/resources/"
)

func TestParsePodDump(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)

	testDumpFile := filepath.Join(ResourceDir, "ui5min.protocol")
	//expectedLog := filepath.Join(ResourceDir, "test.expected.txt")

	t.Run("parse", func(t *testing.T) {
		res, err := ParsePodTcpDump(ctx, TcpFile{"ui5min.protocol", testDumpFile})
		assert.Nil(t, err)
		assert.Equal(t, int64(1669457), res.Size)
		assert.Equal(t, uint64(100605), res.ProtocolVersion)
		assert.Equal(t, "ops-profiler", res.Namespace)
		assert.Equal(t, "esc-ui-service", res.Microservice)
		assert.Equal(t, "esc-ui-service-8dd5b49fd-2gr2g_1691167327796", res.PodName)
		assert.Equal(t, 8, len(res.Streams))

		streams := map[string]string{
			"trace":      "30:35:6C:4C:79:D5:41:DA:BB:8C:92:8D:01:6D:1E:A1",
			"calls":      "A6:12:85:9D:A6:73:4C:90:B1:99:63:D0:8B:9A:DE:E4",
			"xml":        "B5:D3:77:F2:A2:E0:41:ED:95:4B:DC:7C:5D:97:34:71",
			"sql":        "69:7B:12:3B:31:7F:48:ED:BE:07:69:DD:99:92:9B:9D",
			"dictionary": "95:D3:22:0F:CD:08:41:55:B3:D0:79:A0:06:D9:EB:C6",
			"suspend":    "71:B3:A9:A4:89:3C:4F:9A:A1:F5:00:5D:E7:BC:19:D5",
			"gc":         "03:66:27:1D:47:97:4E:30:B9:A5:3D:86:E9:8B:D7:BD",
			"params":     "8A:43:7A:F2:58:74:47:73:8F:F2:CD:5B:83:FD:83:F8",
		}
		for s, handle := range streams {
			assert.Equal(t, handle, res.StreamTypes[s].Str)
			assert.Equal(t, s, res.Streams[handle].StreamType)
			if s != "dictionary" && s != "params" {
				assert.Equal(t, uint64(3600000), res.Streams[handle].RotationPeriod)
				assert.Equal(t, uint64(2097152), res.Streams[handle].RotationSize)
			} else {
				assert.Equal(t, uint64(0), res.Streams[handle].RotationPeriod)
				assert.Equal(t, uint64(0), res.Streams[handle].RotationSize)
			}
		}
	})
}
