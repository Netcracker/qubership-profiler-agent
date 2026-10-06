package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/handlers"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/utils"
)

func getCommandName() string {
	return flag.Arg(0)
}

const (
	commandTimeout = 30 * time.Second // timeout for execution any command
)

// --------------------------------------------------------------------------------------------------------

const mainUsageMessage = `
subcommands:
heap [zip upload]
   collects heap dump for java pid where 'zip' and 'upload' are optional flags.
   'upload' one works together with zip flag only.
   example: diagtools zip or diagtools zip upload
dump
   collects java thread dump and CPU usage for java pid
   example: diagtools dump
scan [list of file patterns]
   finds files matching list of patterns, zip (if necessary) ".hprof" and uploads ".hprof.zip" and other found files.
   example: diagtools scan "${NC_DIAGNOSTIC_LOG_FOLDER}"/*.hprof* ./core* ./hs_err*
schedule
   collects dumps(like dump subcommand), uploads ".hprof.zip" and other found files, scans (like scan subcommand) and
   clears logs located in NC_DIAGNOSTIC_LOGS_FOLDER by schedule.
   Interval can be changed via DIAGNOSTIC_DUMP_INTERVAL(default 1m), DIAGNOSTIC_SCAN_INTERVAL(default 3m) and
   KEEP_LOGS_INTERVAL(default 2 days) environment variables.
   example: diagtools schedule
zkConfig 'path to zookeeper property file' 'zookeeper properties which are to be changed'
   changes nc-diagnostic-agent settings in case of ZOOKEEPER_ENABLED=true
   The first parameter is a path to zookeeper property file.
   The second and further ones are the zookeeper properties which are to be changed.
   example: diagtools zkConfig "${NC_DIAGNOSTIC_FOLDER}/zkproperties" esc.config NC_DIAGNOSTIC_ESC_ENABLED
`

func mainUsage() {
	w := flag.CommandLine.Output()
	_, _ = fmt.Fprint(w, "Usage:\nglobal options:\n")
	flag.PrintDefaults()
	_, _ = fmt.Fprint(w, mainUsageMessage)
}

// --------------------------------------------------------------------------------------------------------

func scheduleModifier(baseCtx context.Context, done chan struct{}) {
	logPath := constants.LogFolder()
	// E.g.: ./diagtools schedule
	cmdCtx := log.ChildCtx(baseCtx, "schedule")

	go func(ctx context.Context) {
		if err := handlers.HandleScheduleCmd(ctx, logPath); err != nil {
			log.Error(ctx, err, "Schedule request failed")
		}
		done <- struct{}{}
	}(cmdCtx)
}

// --------------------------------------------------------------------------------------------------------

func dumpModifier(baseCtx context.Context, done chan struct{}) {
	// E.g.: ./diagtools dump
	cmdCtx := log.ChildCtx(baseCtx, "dump")

	file, err := utils.Flock(utils.GetLockFileName())
	if err != nil {
		log.Error(cmdCtx, err, "Can't handle dump request, problem with lock file")
		os.Exit(1)
	}
	go func(ctx context.Context) {
		err = handlers.HandleDumpCmd(ctx)
		if err != nil {
			log.Error(ctx, err, "Dump request failed")
		}
		err = utils.Unlock(file)
		if err != nil {
			log.Error(cmdCtx, err, "file unlock failed")
		}
		done <- struct{}{}
	}(cmdCtx)
}

// --------------------------------------------------------------------------------------------------------

const heapUsageMessage = `
Usage:
  diagtools heap [zip upload]
  collects heap dump for java pid where 'zip' and 'upload' are optional flags:
     - zip responsible for zipping heap dump
     - upload responsible for uploading zipped heap dump to collector.
       Works together with zip flag only.
`

func heapUsage() {
	w := flag.CommandLine.Output()
	_, _ = fmt.Fprint(w, heapUsageMessage)
}

func heapModifier(baseCtx context.Context, done chan struct{}) {
	// E.g.: ./diagtools heap zip upload
	handlers.LockedSubCommand(baseCtx, "heap", done, heapUsage, handlers.HandleHeapDumpCmd)
}

// --------------------------------------------------------------------------------------------------------

const scanUsageMessage = `
Usage:
   diagtools scan [list of of file patterns]
   finds files matching list of patterns, zip (if necessary) ".hprof" and
   uploads ".hprof.zip" and other found files to collector.
   example: diagtools scan "${NC_DIAGNOSTIC_LOG_FOLDER}"/*.hprof* ./core* ./hs_err* ...
`

func scanUsage() {
	w := flag.CommandLine.Output()
	_, _ = fmt.Fprint(w, scanUsageMessage)
}

func scanModifier(baseCtx context.Context, done chan struct{}) {
	// E.g.: ./diagtools scan "${NC_DIAGNOSTIC_LOG_FOLDER}"/*.hprof* ./core* ./hs_err*
	handlers.LockedSubCommand(baseCtx, "scan", done, scanUsage, handlers.HandleScanCmd)
}

// --------------------------------------------------------------------------------------------------------

const consulConfigUsageMessage = `
Usage:
   diagtools consulCfg 'path to property file' 'properties which are to be changed'
   changes nc-diagnostic-agent settings in case of CONSUL_ENABLED=true
   The first parameter is a path to property file.
   The second and further ones are the properties which are to be changed.
   example: diagtools consulCfg "${NC_DIAGNOSTIC_FOLDER}/properties" esc.config NC_DIAGNOSTIC_ESC_ENABLED
`

func consulConfigUsage() {
	w := flag.CommandLine.Output()
	_, _ = fmt.Fprint(w, consulConfigUsageMessage)
}

func consulConfigModifier(baseCtx context.Context, done chan struct{}) {
	// E.g.: ./diagtools consulCfg "${NC_DIAGNOSTIC_FOLDER}/properties" esc.config NC_DIAGNOSTIC_ESC_ENABLED ...
	handlers.SubCommand(baseCtx, "consulCfg", done, consulConfigUsage, handlers.HandleConsulConfigCmd)
}

// --------------------------------------------------------------------------------------------------------

const serverConfigUsageMessage = `
Usage:
   diagtools serverCfg 'path to property file' 'properties which are to be changed'
   changes nc-diagnostic-agent settings in case of CONFIG_SERVER is not empty
   The first parameter is a path to property file.
   The second and further ones are the properties which are to be changed.
   example: diagtools serverCfg "${NC_DIAGNOSTIC_FOLDER}/properties" esc.config NC_DIAGNOSTIC_ESC_ENABLED
`

func configServerUsage() {
	w := flag.CommandLine.Output()
	_, _ = fmt.Fprint(w, serverConfigUsageMessage)
}

func cmdConfigServerModifier(baseCtx context.Context, done chan struct{}) {
	// E.g.: ./diagtools serverCfg "${NC_DIAGNOSTIC_FOLDER}/properties" esc.config NC_DIAGNOSTIC_ESC_ENABLED ...
	handlers.SubCommand(baseCtx, "serverCfg", done, configServerUsage, handlers.HandleConfigServerCmd)
}

// --------------------------------------------------------------------------------------------------------

const zkConfigUsageMessage = `
Usage:
   diagtools zkConfig 'path to property file' 'properties which are to be changed'
   changes nc-diagnostic-agent settings in case of ZOOKEEPER_ENABLED=true
   The first parameter is a path to property file.
   The second and further ones are the properties which are to be changed.
   example: diagtools zkCfg "${NC_DIAGNOSTIC_FOLDER}/properties" esc.config NC_DIAGNOSTIC_ESC_ENABLED
`

func zkConfigUsage() {
	w := flag.CommandLine.Output()
	_, _ = fmt.Fprint(w, zkConfigUsageMessage)
}

func cmdZkConfigModifier(baseCtx context.Context, done chan struct{}) {
	// E.g.: ./diagtools zkCfg "${NC_DIAGNOSTIC_FOLDER}/properties" esc.config NC_DIAGNOSTIC_ESC_ENABLED ...
	handlers.SubCommand(baseCtx, "zkCfg", done, zkConfigUsage, handlers.HandleZkConfigCmd)
}

// --------------------------------------------------------------------------------------------------------

func prepareLogFilename(cmdName string) string {
	path, err := os.Executable()
	if err != nil {
		panic(err)
	}
	execName := filepath.Base(path)

	var builder strings.Builder
	builder.WriteString(execName)
	builder.WriteString("_")
	builder.WriteString(cmdName)
	builder.WriteString(".log")
	filename := builder.String()
	return filename
}

func CreateLogger(cmdLogFilename string) {
	logPath := constants.LogFolder()
	logFilename := filepath.Join(logPath, constants.NcDiagLogSubFolder, cmdLogFilename)
	logFilename = filepath.Clean(logFilename)

	logLevel := constants.GetLogLevelFromEnv()
	logInterval := constants.LogInterval()
	fileSize := constants.LogFileSize()
	fileBackups := constants.LogFileBackups()
	printToConsole := constants.LogPrintToConsole()
	log.SetupLogger(log.CreateLogger(logLevel, printToConsole, logFilename, fileSize, fileBackups, logInterval))
}

// --------------------------------------------------------------------------------------------------------

var cpuProfile = flag.String("cpu_prof", "", "write cpu profile to file")
var memProfile = flag.String("mem_prof", "", "write mem profile to file")

func main() {
	flag.Usage = mainUsage
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}

	baseCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	baseCtx = log.ChildCtx(baseCtx, "main")

	cmdName := getCommandName()
	execFilename := prepareLogFilename(cmdName)
	CreateLogger(execFilename)

	cancelPprof := utils.RunCpuProfile(baseCtx, *cpuProfile)
	utils.StartHeapProfile(baseCtx, *memProfile)

	var done, finishUP = make(chan struct{}), make(chan struct{})

	go func() {
		<-baseCtx.Done()
		log.Error(baseCtx, baseCtx.Err(), "diagtool is forced stopping")
		// send message on "finish up" channel to gracefully shutdown
		finishUP <- struct{}{}
	}()

	switch handlers.AsType(cmdName) {
	case handlers.HeapModifier:
		heapModifier(baseCtx, done)
	case handlers.DumpModifier:
		dumpModifier(baseCtx, done)
	case handlers.ScanModifier:
		scanModifier(baseCtx, done)
	case handlers.ScheduleModifier:
		scheduleModifier(baseCtx, done)
	case handlers.ConsulConfigModifier:
		consulConfigModifier(baseCtx, done)
	case handlers.ConfigServerModifier:
		cmdConfigServerModifier(baseCtx, done)
	case handlers.ZkConfigModifier:
		cmdZkConfigModifier(baseCtx, done)
	default:
		var err error
		err = fmt.Errorf("unknown command : %s", cmdName)
		log.Error(baseCtx, err, "failed to execute cmd request")
		os.Exit(1)
	}

	var returnCode int
	select {
	case <-finishUP:
		select {
		case <-time.After(commandTimeout):
			returnCode = 1
			log.Info(baseCtx, "diagtool is going to finish up by timeout")
		case <-done:
			log.Info(baseCtx, "diagtool is going to finish up gracefully")
		}
	case <-done:
		log.Info(baseCtx, "diagtool is going to shut down gracefully")
	}

	utils.PersistHeapProfile(baseCtx, *memProfile)
	if cancelPprof != nil {
		cancelPprof()
	}

	log.Infof(baseCtx, "diagtool has shut down with code %d", returnCode)
	os.Exit(returnCode)
}
