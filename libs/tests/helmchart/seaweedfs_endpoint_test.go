package helmchart

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seaweedfsArgs turns the subchart on, clears the explicit endpoint that
// renderChart sets, and gives SeaweedFS the key pair the chart requires to
// match s3.auth.
var seaweedfsArgs = []string{
	"--set", "s3.endpoint=",
	"--set", "seaweedfs.enabled=true",
	"--set", "seaweedfs.s3.credentials.admin.accessKey=test",
	"--set", "seaweedfs.s3.credentials.admin.secretKey=test",
}

// perComponent replaces the all-in-one pod with the per-component workloads
// that the standalone and filer-hosted gateways run next to.
var perComponent = []string{
	"--set", "seaweedfs.allInOne.enabled=false",
	"--set", "seaweedfs.master.enabled=true",
	"--set", "seaweedfs.volume.enabled=true",
	"--set", "seaweedfs.filer.enabled=true",
}

var (
	s3EndpointLine = regexp.MustCompile(`(?m)^  s3-endpoint: "http://([^:"]+):(\d+)"\r?$`)
	s3PortLine     = regexp.MustCompile(`- name: "swfs-s3"\n\s+port: (\d+)\n`)
)

// serviceS3Port returns the swfs-s3 port of the Service named name in the
// rendered manifests. It searches that Service's own YAML document only, so a
// Service without an S3 port is reported as missing instead of borrowing the
// port of a Service rendered after it.
func serviceS3Port(manifests, name string) (string, bool) {
	header := fmt.Sprintf("\nkind: Service\nmetadata:\n  name: %s\n", name)
	for _, doc := range strings.Split(manifests, "\n---\n") {
		if !strings.Contains(doc, header) {
			continue
		}
		if m := s3PortLine.FindStringSubmatch(doc); m != nil {
			return m[1], true
		}
		return "", false
	}
	return "", false
}

// TestSeaweedfsEndpointMatchesGatewayService checks that the default
// s3-endpoint names a Service the subchart renders, on the port that Service
// exposes for S3. The subchart runs the gateway in the all-in-one pod, as a
// standalone Deployment, or inside the filer, and each mode reads its port
// from a different key.
func TestSeaweedfsEndpointMatchesGatewayService(t *testing.T) {
	tests := []struct {
		name string
		args []string
		host string
		port string
	}{
		{
			name: "all-in-one on the default port",
			host: "render-test-seaweedfs-all-in-one",
			port: "8333",
		},
		{
			name: "all-in-one on its own port",
			args: []string{"--set", "seaweedfs.allInOne.s3.port=9222", "--set", "seaweedfs.s3.port=9444"},
			host: "render-test-seaweedfs-all-in-one",
			port: "9222",
		},
		{
			name: "all-in-one inherits s3.port",
			args: []string{"--set", "seaweedfs.s3.port=9444"},
			host: "render-test-seaweedfs-all-in-one",
			port: "9444",
		},
		{
			name: "standalone gateway ignores the all-in-one port",
			args: append([]string{
				"--set", "seaweedfs.s3.enabled=true",
				"--set", "seaweedfs.s3.port=9444",
				"--set", "seaweedfs.allInOne.s3.port=9222",
			}, perComponent...),
			host: "render-test-seaweedfs-s3",
			port: "9444",
		},
		{
			// The filer stores through the all-in-one pod's master; the
			// subchart renders no master of its own with these values.
			name: "standalone gateway beside an all-in-one pod without one",
			args: []string{
				"--set", "seaweedfs.allInOne.s3.enabled=false",
				"--set", "seaweedfs.filer.enabled=true",
				"--set", "seaweedfs.s3.enabled=true",
				"--set", "global.seaweedfs.masterServer=render-test-seaweedfs-all-in-one:9333",
			},
			host: "render-test-seaweedfs-s3",
			port: "8333",
		},
		{
			name: "standalone gateway wins over a filer-hosted one",
			args: append([]string{
				"--set", "seaweedfs.s3.enabled=true",
				"--set", "seaweedfs.s3.port=9444",
				"--set", "seaweedfs.filer.s3.enabled=true",
				"--set", "seaweedfs.filer.s3.port=9555",
			}, perComponent...),
			host: "render-test-seaweedfs-s3",
			port: "9444",
		},
		{
			name: "filer-hosted gateway",
			args: append([]string{
				"--set", "seaweedfs.filer.s3.enabled=true",
				"--set", "seaweedfs.filer.s3.port=9444",
			}, perComponent...),
			host: "render-test-seaweedfs-s3",
			port: "9444",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append(append([]string{}, seaweedfsArgs...), tt.args...)
			out := renderChart(t, args...)

			endpoint := s3EndpointLine.FindStringSubmatch(out)
			require.NotNil(t, endpoint, "the ConfigMap must carry an s3-endpoint")
			assert.Equal(t, tt.host, endpoint[1])
			assert.Equal(t, tt.port, endpoint[2])

			port, ok := serviceS3Port(out, endpoint[1])
			require.True(t, ok, "the subchart must render Service %s with an S3 port", endpoint[1])
			assert.Equal(t, endpoint[2], port, "the endpoint port must be the one the Service exposes")
		})
	}
}

// TestSeaweedfsWithoutGatewayFailsRender checks that the render stops when the
// subchart is on but no pod runs an S3 gateway, instead of pointing the
// workloads at a gateway that cannot serve. The subchart renders the s3
// Service for s3.enabled or filer.s3.enabled even without the filer, which
// both gateways need: the filer-hosted one runs in the filer pod, and the
// standalone one connects to the filer-client Service.
func TestSeaweedfsWithoutGatewayFailsRender(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "all-in-one without its gateway",
			args: []string{"--set", "seaweedfs.allInOne.s3.enabled=false"},
		},
		{
			name: "standalone gateway without the filer",
			args: []string{
				"--set", "seaweedfs.allInOne.s3.enabled=false",
				"--set", "seaweedfs.s3.enabled=true",
			},
		},
		{
			name: "filer-hosted gateway without the filer",
			args: []string{
				"--set", "seaweedfs.allInOne.s3.enabled=false",
				"--set", "seaweedfs.filer.s3.enabled=true",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append(append([]string{}, seaweedfsArgs...), tt.args...)
			out, err := renderChartE(t, args...)
			require.Error(t, err, "helm template must fail: %s", out)
			assert.Contains(t, out, "the subchart runs no S3 gateway")
		})
	}
}
