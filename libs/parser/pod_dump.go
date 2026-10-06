package parser

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"sort"

	"github.com/Netcracker/qubership-profiler-backend/libs/parser/streams"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"

	"github.com/Netcracker/qubership-profiler-backend/libs/files"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type ParsedPodDump struct {
	*LoadedTcpData
	Params     *data.Params
	Dictionary *data.Dictionary
	Suspend    *data.Suspends
	Traces     *data.Traces
	Calls      *data.Calls
}

func (p *ParsedPodDump) ParamsChunk() *model.Chunk {
	return p.ByType("params")
}

func (p *ParsedPodDump) DictionaryChunk() *model.Chunk {
	if p.ProtocolVersion == uint64(100705) {
		return p.ByType("posDictionary")
	}
	return p.ByType("dictionary")
}

func (p *ParsedPodDump) LatestXmlChunk() *model.Chunk {
	return p.ByType("xml")
}

func (p *ParsedPodDump) LatestSqlChunk() *model.Chunk {
	return p.ByType("sql")
}

func (p *ParsedPodDump) LatestTraceChunk() *model.Chunk {
	return p.ByType("trace")
}

func (p *ParsedPodDump) LatestCallsChunk() *model.Chunk {
	return p.ByType("calls")
}

func (p *ParsedPodDump) ByType(streamType string) *model.Chunk {
	return p.Streams[p.StreamTypes[streamType].String()]
}

func (p *ParsedPodDump) Name() string {
	return fmt.Sprintf("%v:%v:%v", p.Namespace, p.Microservice, p.PodName)
}

func (p *ParsedPodDump) ParseStreams(ctx context.Context, saveFiles bool, outputDir string) {
	log.Debug(ctx, "total %d chunks series", len(p.Streams))
	i := 0
	sum := 0

	list := p.StreamsList()
	log.Info(ctx, "streams for pod '%s': %v", p.PodName, list)

	if saveFiles {
		err := files.ClearDirectory(ctx, outputDir)
		if err == nil {
			err = files.CheckDir(ctx, outputDir)
		}
		if err != nil {
			log.Error(ctx, err, "could not create directory: %v", outputDir)
			return
		}
	}

	for _, stream := range list {
		var c *model.Chunk
		for _, v := range p.Streams {
			if v.StreamType == stream {
				c = v
				break
			}
		}
		if c == nil {
			continue
		}
		log.Debug(ctx, "%d. %v", i, c.String())

		streamFileName := func(ext string) string {
			return fmt.Sprintf("%s/%s.%s.%d.%s", outputDir, p.PodName, c.StreamType, c.SequenceId, ext)
		}
		writeFile := func(fileExtension string, data []byte) {
			if saveFiles {
				fileName := streamFileName(fileExtension)
				_ = os.WriteFile(fileName, data, fs.ModePerm)
			}
		}
		writeText := func(s string) {
			writeFile("txt", []byte(s))
		}

		writeFile("bin", c.Bytes())
		i++
		sum += c.Size()

		txtLog := ""
		var err error
		if c.StreamType == "sql" || c.StreamType == "xml" {
			txtLog = streams.ReadStringStream(ctx, c.StreamType, c)
			writeText(txtLog)
		}
		if c.StreamType == "dictionary" {
			p.Dictionary, txtLog, err = streams.ReadDictionary(ctx, c)
			writeText(txtLog)
		}
		if c.StreamType == "params" {
			p.Params, txtLog, err = streams.ReadParams(ctx, c)
			writeText(txtLog)
		}
		if c.StreamType == "suspend" {
			p.Suspend, txtLog, err = streams.ReadSuspend(ctx, c)
			writeText(txtLog)
		}
		if c.StreamType == "calls" {
			p.Calls, txtLog, err = streams.ReadCalls(ctx, c)
			writeText(txtLog)
		}
		if c.StreamType == "trace" {
			p.Traces, txtLog, err = streams.ReadTraces(ctx, c, p.Dictionary)
			writeText(txtLog)
		}
		if err != nil {
			break
		}
	}
	log.Debug(ctx, "total chunks size: %d bytes", sum)

	PodStat(ctx, p)
}

func (p *ParsedPodDump) StreamsList() (list []model.StreamType) {
	for _, c := range p.Streams {
		list = append(list, c.StreamType)
	}
	sort.Strings(list)
	return list
}
