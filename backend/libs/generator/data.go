package generator

import (
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common/formats"

	"github.com/Netcracker/qubership-profiler-backend/libs/parser"
)

type (
	// Pod information about generated pod
	Pod struct {
		Namespace string
		Service   string
		PodName   string
		Restart   time.Time
		Dumps     PodDumps // will send same dumps regularly
	}

	// PodDumps randomly selected from found templates
	PodDumps struct {
		Td  *DumpFile
		Top *DumpFile
		Tcp *parser.ParsedPodDump
	}
)

// PrepareDumpPath generate URI-path to send dump to the collector
func (p *Pod) PrepareDumpPath(ts time.Time, dumpType string) string {
	var dateFile = ts.Format(formats.DateTimeFile)
	var datePath = ts.Format(formats.DateTimeDir)
	return fmt.Sprintf(`%s/%s/%s/%s.%s.txt`, p.Namespace, datePath, p.PodName, dateFile, dumpType)
}

// PreparePath generate path for current time
func (p *Pod) PreparePath(dumpType string) string {
	return p.PrepareDumpPath(time.Now(), dumpType)
}
