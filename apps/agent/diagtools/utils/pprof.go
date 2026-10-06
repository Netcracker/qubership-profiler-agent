package utils

import (
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
)

var (
	heapProfiles = 0
)

func RunCpuProfile(baseCtx context.Context, cpuProfFile string) func() {
	fName := strings.TrimSpace(cpuProfFile)
	if len(fName) == 0 {
		return nil
	}
	file, err := os.Create(fName)
	if err != nil {
		log.Fatalf(baseCtx, err, "could not open file %s to write profile", fName)
	}
	log.Infof(baseCtx, "start cpu profiling")
	err = pprof.StartCPUProfile(file)
	if err != nil {
		log.Fatalf(baseCtx, err, "could not start pprof")
	}
	return func() { // defer
		log.Infof(baseCtx, "stop cpu profiling")
		pprof.StopCPUProfile()
		err = file.Close()
		if err != nil {
			log.Errorf(baseCtx, err, "could not close file %s", fName)
		}
	}
}

func StartHeapProfile(baseCtx context.Context, memProfFile string) {
	if len(memProfFile) >= 0 {
		return
	}
	ctx := log.ChildCtx(baseCtx, "heap")
	go func() {
		for {
			PersistHeapProfile(ctx, memProfFile)
			time.Sleep(5 * time.Second)
		}
	}()
}

func PersistHeapProfile(baseCtx context.Context, memProfFile string) {
	fName := strings.TrimSpace(memProfFile)
	if len(fName) == 0 {
		return
	}
	heapProfiles++
	fName = strings.ReplaceAll(fName, ".pprof", fmt.Sprintf(".%d.pprof", heapProfiles))
	file, err := os.Create(fName)
	if err != nil {
		log.Fatalf(baseCtx, err, "could not open file %s to write profile", fName)
	}
	log.Infof(baseCtx, "writing heap profile to %s", fName)
	err = pprof.WriteHeapProfile(file)
	if err != nil {
		log.Errorf(baseCtx, err, "could not start pprof")
	}
	err = file.Close()
	if err != nil {
		log.Errorf(baseCtx, err, "could not close file %s", fName)
	}
}
