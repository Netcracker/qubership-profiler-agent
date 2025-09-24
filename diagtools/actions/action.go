package actions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/utils"

	"github.com/shirou/gopsutil/v3/process"
)

type Action struct {
	DcdEnabled  bool
	CmdTimeout  time.Duration
	Pid         string
	PidName     string
	Command     string
	DumpPath    string
	ZipDumpPath string
	PodName     string
	TargetUrl   string
	CmdArgs     []string
}

func (action *Action) GetPid(ctx context.Context) (pid string, err error) {
	var processes []*process.Process
	ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	processes, err = process.ProcessesWithContext(ctxTimeout)
	if errors.Is(ctxTimeout.Err(), context.DeadlineExceeded) {
		log.Errorf(ctx, ctxTimeout.Err(),
			"command:%s %s, get processes timeout.", action.Command, action.CmdArgs)
		return "", ctxTimeout.Err()
	}
	if err != nil {
		log.Error(ctx, err, "failed to get currently running processes")
		return
	}
	for _, proc := range processes {
		var procName string
		procName, err = proc.Name()
		if err != nil {
			if strings.Contains(err.Error(), "Access is denied") { // special case for win
				continue
			}
			log.Error(ctx, err, "failed to get name of the java process")
			return
		}
		//log.Debugf(ctx, "proc %-5s - %v", action.Pid, procName)
		if strings.Contains(procName, action.PidName) {
			pid = strconv.FormatInt(int64(proc.Pid), 10)
			break
		}
	}

	if pid == "" {
		err = errors.New("failed to get java process pid")
		log.Error(ctx, err, "no any java process started")
	}

	return
}

func (action *Action) GetDumpFile(extension string) (err error) {
	filename := time.Now().UTC().Format(constants.DumpFileTimestampLayout) //time.DateTime

	var builder strings.Builder
	builder.WriteString(filename)
	builder.WriteString(extension)
	filename = builder.String()

	_, err = os.Stat(action.DumpPath)

	if os.IsNotExist(err) {
		if err = os.Mkdir(action.DumpPath, os.ModePerm); err != nil {
			return
		}
	}

	action.DumpPath = filepath.Join(action.DumpPath, filename)

	return
}

func (action *Action) GetParams() (err error) {
	for i, arg := range action.CmdArgs {
		var params bytes.Buffer
		if strings.Contains(arg, "{{") {
			tmpl := template.New("pTemplate")
			tmpl, _ = tmpl.Parse(arg)
			err = tmpl.Execute(&params, action)
			if err != nil {
				return
			}
			action.CmdArgs[i] = params.String()
		}
	}

	return
}

func (action *Action) RunJCmd(ctx context.Context) (err error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, action.CmdTimeout)
	defer cancel()

	proc := exec.CommandContext(ctxTimeout, action.Command, action.CmdArgs...)

	log.Infof(ctx, "run command '%s %s' for pid #%s", action.Command, strings.Trim(fmt.Sprint(action.CmdArgs), "[]"), action.Pid)

	err = proc.Run()

	if errors.Is(ctxTimeout.Err(), context.DeadlineExceeded) {
		log.Errorf(ctx, ctx.Err(),
			"timeout for process '%s %s'. Stderr:\n%s", action.Command, action.CmdArgs, proc.Stderr)
		return ctxTimeout.Err()
	}
	if err != nil {
		log.Errorf(ctx, err,
			"failed to run '%s %s'. Stderr:\n%s", action.Command, action.CmdArgs, proc.Stderr)
		return
	}

	return
}

func (action *Action) RunJCmdWithOutput(ctx context.Context) (output []byte, err error) {
	ctxTimeout, cancel := context.WithTimeout(ctx, action.CmdTimeout)
	defer cancel()

	log.Infof(ctx, "run command '%s %s' for pid #%s", action.Command, strings.Trim(fmt.Sprint(action.CmdArgs), "[]"), action.Pid)

	proc := exec.CommandContext(ctxTimeout, action.Command, action.CmdArgs...)
	output, err = proc.Output()

	if errors.Is(ctxTimeout.Err(), context.DeadlineExceeded) {
		log.Errorf(ctx, ctxTimeout.Err(),
			"timeout for process '%s %s'. Stderr:\n%s", action.Command, action.CmdArgs, proc.Stderr)
		return nil, ctxTimeout.Err()
	}
	if err != nil {
		log.Errorf(ctx, err,
			"failed to run '%s %s'. Stderr:\n%s", action.Command, action.CmdArgs, proc.Stderr)
		return
	}

	return
}

func (action *Action) UploadOutputToDiagnosticCenter(ctx context.Context, output []byte) error {
	startTime := time.Now()

	reader := bytes.NewReader(output)
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, action.TargetUrl, reader)
	if err != nil {
		log.Error(ctx, err, "failed to create http request")
		return err
	}
	request.Header.Add("Content-Type", "application/octet-stream")

	err = utils.SendFileRequest(ctx, action.DumpPath, request)
	if err == nil {
		log.Info(ctx, fmt.Sprintf("uploaded %s", action.DumpPath),
			"bytes", len(output), "duration", time.Since(startTime))
	}
	return err
}

func (action *Action) ZipDump(ctx context.Context) (err error) {
	action.ZipDumpPath, err = utils.ZipDump(ctx, action.DumpPath, action.ZipDumpPath)
	return
}

func (action *Action) GetTargetUrl(ctx context.Context) (err error) {
	diagService := constants.DiagService(ctx)

	namespace, err := constants.GetNamespace()
	if err != nil {
		log.Errorf(ctx, err, "environment variable %s has to be set up", constants.NcCloudNamespace)
	}

	filename := action.fileName()
	diagDate := action.getDiagDate(filename, time.Now())
	action.TargetUrl, err = url.JoinPath(diagService, constants.NcDiagServicePath, namespace, diagDate, action.PodName, filename)

	return
}

func (action *Action) fileName() string {
	if action.ZipDumpPath != "" {
		return filepath.Base(action.ZipDumpPath)
	} else {
		return filepath.Base(action.DumpPath)
	}
}

func (action *Action) getDiagDate(s string, t time.Time) string {
	s = strings.ReplaceAll(s, ".hprof", "")
	s = strings.ReplaceAll(s, ".zip", "")
	if tt, err := time.Parse(constants.DumpFileTimestampLayout, s); err == nil {
		t = tt
	}
	return t.UTC().Format(constants.NcDiagDateLayout)
}

func GetTargetUrl(ctx context.Context, podName string, dumpPath string) (string, error) {
	diagService := constants.DiagService(ctx)

	namespace, err := constants.GetNamespace()
	if err != nil {
		log.Errorf(ctx, err, "environment variable %s has to be set up", constants.NcCloudNamespace)
	}

	diagDate := time.Now().UTC().Format(constants.NcDiagDateLayout)
	filename := filepath.Base(dumpPath)
	targetUrl, err := url.JoinPath(diagService, constants.NcDiagServicePath, namespace, diagDate, podName, filename)
	return targetUrl, err
}

func (action *Action) GetPodName(ctx context.Context) (err error) {
	err = action.getPodName()
	if err != nil {
		log.Errorf(ctx, err, "could not retrieve pod name")
	}
	return err
}

func (action *Action) getPodName() (err error) {
	var podName []byte
	folder, found := os.LookupEnv(constants.NcDiagnosticFolder)
	if !found {
		folder, found = os.LookupEnv(constants.NcProfilerFolder)
		if !found {
			podName, err = os.ReadFile(constants.DefaultPodNameFilePath)
			if err != nil {
				action.PodName, err = os.Hostname()
				if err != nil {
					return
				}
			} else {
				action.PodName = string(podName)
			}

		} else {
			filename := filepath.Join(folder, constants.DefaultPodNameFile)
			podName, err = os.ReadFile(filename)
			if err != nil {
				action.PodName, err = os.Hostname()
				if err != nil {
					return
				}
			} else {
				action.PodName = string(podName)
			}
		}
	} else {
		filename := filepath.Join(folder, constants.DefaultPodNameFile)
		podName, err = os.ReadFile(filename)
		if err != nil {
			action.PodName, err = os.Hostname()
			if err != nil {
				return
			}
		} else {
			action.PodName = strings.TrimSpace(string(podName))
		}
	}

	return
}
