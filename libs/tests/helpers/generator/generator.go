package generator

import (
	"context"
	"crypto/rand"
	"fmt"
	mRand "math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/storage"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/parser"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
)

type Config struct {
	NumberOfNS       int
	NumberOfServices int
	NumberOfPods     int
	NsPrefix         string
	ServicePrefix    string
	PodPrefix        string
	NumberOfCalls    int // how many calls each pod generates in 5 minutes
	NumberOfDumps    int
	TimeRange        int
	PathToPodFile    string
	PathToDumpsDir   string
}

type Generator struct {
	cfg      *Config
	Time     time.Time
	UnixTime int64 // UTC, milliseconds
	Calls    []*data.Call
	Dumps    []*model.Dump
}

func SimpleConfig(ns, svc, pods int) *Config {
	return &Config{
		NumberOfNS:       ns,
		NumberOfServices: svc,
		NumberOfPods:     pods,
		NsPrefix:         "ns",
		ServicePrefix:    "svc",
		PodPrefix:        "pod",
		NumberOfCalls:    10,
		NumberOfDumps:    1,
		TimeRange:        1,
		PathToPodFile:    filepath.Join("../", "resources", "data", "ui5min.bin"),
		PathToDumpsDir:   filepath.Join("../", "resources", "dumps"),
	}
}

func NewGenerator(cfg *Config, t time.Time) *Generator {
	return &Generator{
		cfg:      cfg,
		Time:     t,
		UnixTime: t.UnixMilli(),
	}
}

func (g *Generator) GenerateCalls(ctx context.Context) {
	log.Debug(ctx, "Start of calls generation for %d min", g.cfg.TimeRange)
	pod := g.loadPodData(ctx)

	generatedCalls := make([]*data.Call, 0, g.cfg.NumberOfPods*len(pod.Calls.List))

	for i := 0; i < g.cfg.NumberOfNS; i++ {
		namespace := fmt.Sprintf("%s-%d", g.cfg.NsPrefix, i)
		for j := 0; j < g.cfg.NumberOfServices; j++ {
			service := fmt.Sprintf("%s-%d", g.cfg.ServicePrefix, j)
			for k := 0; k < g.cfg.NumberOfPods; k++ {
				podName := fmt.Sprintf("%s-%d", g.cfg.PodPrefix, k)

				podsCalls := g.generatePodCalls(ctx, pod, g.cfg.TimeRange, g.cfg.NumberOfCalls,
					namespace, service, podName, g.Time)
				generatedCalls = append(generatedCalls, podsCalls...)
			}
		}
	}

	log.Debug(ctx, "Calls for %d min are generated successfully", g.cfg.TimeRange)
	g.Calls = generatedCalls
}

func (g *Generator) loadPodData(ctx context.Context) *parser.ParsedPodDump {
	fileName, err := filepath.Abs(g.cfg.PathToPodFile)
	if err != nil {
		log.Fatal(ctx, err, "invalid path to test data: %s", g.cfg.PathToPodFile)
	}

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(ctx, err, "couldn't open file with test data: %s", fileName)
	}

	tcpFile := parser.TcpFile{FileName: file.Name(), FilePath: fileName}
	data, err := parser.ParsePodTcpDump(ctx, tcpFile)
	if err != nil {
		log.Fatal(ctx, err, "couldn't parse test data: %s", fileName)
	}

	pod := &parser.ParsedPodDump{LoadedTcpData: data}
	if pod != nil {
		pod.ParseStreams(ctx, false, "")
	}
	return pod
}

func (g *Generator) generatePodCalls(ctx context.Context, pod *parser.ParsedPodDump,
	timerange int, callsCount int, namespace, service, podName string, t time.Time) []*data.Call {

	var result []*data.Call
	callsInDump := len(pod.Calls.List)

	maximumCycleLimit := (((timerange / 5) * callsCount) / callsInDump) + 1 // have to have that one step, otherwise get less calls when we need.
	period := time.Duration(timerange) * time.Minute

	callInfos := make([]*data.CallInfo, 0, callsCount)
	for i := 0; i < maximumCycleLimit; i++ {
		callInfos = append(callInfos, pod.Calls.List...)
	}

	for i := 0; i < len(callInfos); i++ {
		call := callInfos[i].Call
		call.Namespace = namespace
		call.ServiceName = service
		call.PodName = podName
		tt := t.Add(time.Duration(i) * period / time.Duration(len(callInfos)))
		call.Time = tt.UnixMilli()
		call.RestartTime = call.Time

		b := make([]byte, 1024)
		_, _ = rand.Read(b)
		call.Trace = b
		result = append(result, &call)
		if len(result) >= callsCount {
			break
		}
	}
	log.Debug(ctx, "generated %d calls for [%s/%s/%s]", len(result), namespace, service, podName)
	return result
}

func (g *Generator) GenerateDumps(ctx context.Context) {
	log.Debug(ctx, "Start of dumps generation for %d min", g.cfg.TimeRange)

	var sourceDumps = map[model.DumpType]string{
		"td":        "dumps.td",
		"top":       "dumps.top",
		"gc":        "dumps.gc",
		"alloc":     "dumps.alloc",
		"goroutine": "dumps.goroutine",
		"heap":      "dumps.heap",
		"profile":   "dumps.profile",
	}

	var podTypeDumps = map[model.DumpType]model.PodType{
		"td":        "java",
		"top":       "java",
		"gc":        "java",
		"alloc":     "go",
		"goroutine": "go",
		"heap":      "go",
		"profile":   "go",
	}

	var result []*model.Dump

	for i := 0; i < g.cfg.NumberOfNS; i++ {
		ns := fmt.Sprintf("%s-%d", g.cfg.NsPrefix, i)
		for dumpType, dirName := range sourceDumps {
			dumps := g.collectDumps(ctx, ns, podTypeDumps[dumpType], dumpType, dirName, g.Time)
			result = append(result, dumps...)
		}
	}

	log.Debug(ctx, "Dumps for %d min are generated successfully", g.cfg.TimeRange)
	g.Dumps = result
}

func (g *Generator) collectDumps(ctx context.Context, ns string, podType model.PodType, dumpType model.DumpType,
	dirName string, t time.Time) []*model.Dump {

	dumps := make([]*model.Dump, 0, g.cfg.NumberOfNS*g.cfg.NumberOfServices*g.cfg.NumberOfPods*g.cfg.NumberOfDumps)
	for j := 0; j < g.cfg.NumberOfServices; j++ {
		service := fmt.Sprintf("%s_%d", g.cfg.ServicePrefix, j)
		for k := 0; k < g.cfg.NumberOfPods; k++ {
			podName := fmt.Sprintf("%s_%d", g.cfg.PodPrefix, k)

			ds, err := g.generateDumps(ns, service, podName, podType, dumpType, dirName, t)
			if err != nil {
				log.Fatal(ctx, err, "Can't create new DumpParquet")
			}
			dumps = append(dumps, ds...)
		}
	}

	return dumps
}

func (g *Generator) generateDumps(namespace, service, pod string, podType model.PodType, dumpType model.DumpType,
	dirName string, t time.Time) ([]*model.Dump, error) {

	l := g.cfg.NumberOfDumps * g.cfg.TimeRange
	dumps := make([]*model.Dump, 0, l)
	for i := 0; i < l; i++ {
		pathToSource := fmt.Sprintf("%s/%s", g.cfg.PathToDumpsDir, dirName)
		d, err := g.newDump(namespace, service, pod, podType, dumpType, pathToSource, t)
		if err != nil {
			return nil, err
		}
		dumps = append(dumps, d)
	}

	return dumps, nil
}

func (g *Generator) newDump(namespace, service, pod string, podType model.PodType, dumpType model.DumpType,
	pathToSource string, t time.Time) (*model.Dump, error) {

	uuid := common.RandomUuid()
	dp := &model.Dump{
		UUID:        uuid,
		Namespace:   namespace,
		ServiceName: service,
		PodName:     pod,
		DumpType:    dumpType,
		PodType:     podType,
	}

	period := time.Duration(g.cfg.TimeRange) * time.Minute
	dp.CreatedTime = t.Add(time.Duration(g.cfg.TimeRange) * period)
	dp.RestartTime = dp.CreatedTime

	filesDir := pathToSource

	files, err := os.ReadDir(filesDir)
	if err != nil {
		return nil, err
	}

	randomFile := files[mRand.Int31n(int32(len(files)))]
	body, err := os.ReadFile(filesDir + "/" + randomFile.Name())
	if err != nil {
		return nil, err
	}

	dp.BinaryData = body
	dp.BytesSize = int64(len(dp.BinaryData))

	return dp, nil
}
