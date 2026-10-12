package helmchart

import (
	"fmt"
	"regexp"
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
	serviceS3Port  = `(?s)\nkind: Service\nmetadata:\n  name: %s\n.*?- name: "swfs-s3"\n\s+port: (\d+)\n`
)

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

			service := regexp.MustCompile(fmt.Sprintf(serviceS3Port, regexp.QuoteMeta(endpoint[1]))).
				FindStringSubmatch(out)
			require.NotNil(t, service, "the subchart must render Service %s with an S3 port", endpoint[1])
			assert.Equal(t, endpoint[2], service[1], "the endpoint port must be the one the Service exposes")
		})
	}
}

// TestSeaweedfsWithoutGatewayFailsRender checks that the render stops when the
// subchart is on but no gateway mode is, instead of pointing the workloads at
// a Service that does not exist.
func TestSeaweedfsWithoutGatewayFailsRender(t *testing.T) {
	args := append(append([]string{}, seaweedfsArgs...), "--set", "seaweedfs.allInOne.s3.enabled=false")
	out, err := renderChartE(t, args...)
	require.Error(t, err, "helm template must fail: %s", out)
	assert.Contains(t, out, "the subchart runs no S3 gateway")
}
