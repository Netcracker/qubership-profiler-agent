package handlers

import (
	"context"
	"flag"
	"io"
	"os"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/utils"
)

// --------------------------------------------------------------------------------------------------------

type SubCommandFunc = func(context.Context, []string) error

var (
	testArgs   []string
	testOutput io.Writer
)

func subArgs() []string {
	if testArgs != nil {
		return testArgs
	}

	args := flag.Args()
	if len(args) > 0 {
		args = args[1:]
	}
	return args
}

// SubCommand run command as goroutine
func SubCommand(baseCtx context.Context, cmdName string, done chan struct{}, usage func(), f SubCommandFunc) {
	cmdCtx := log.ChildCtx(baseCtx, cmdName)

	flagSet := flag.NewFlagSet(cmdName, flag.ExitOnError)
	flagSet.Usage = usage
	if testOutput != nil {
		flagSet.SetOutput(testOutput)
	}
	err := flagSet.Parse(subArgs())
	if err != nil {
		log.Errorf(cmdCtx, err, "invalid parameters for command: %v", flagSet.Args())
		os.Exit(1)
	} else {
		go func(ctx context.Context) {
			err = f(ctx, flagSet.Args())
			if err != nil {
				log.Errorf(ctx, err, "command %s failed", cmdName)
			}
			done <- struct{}{}
		}(cmdCtx)
	}
}

// LockedSubCommand run command as goroutine in lock mode (should not interfere with other instances)
func LockedSubCommand(baseCtx context.Context, cmdName string, done chan struct{}, usage func(), f SubCommandFunc) {
	cmdCtx := log.ChildCtx(baseCtx, cmdName)

	file, err := utils.Flock(utils.GetLockFileName())
	if err != nil {
		log.Error(cmdCtx, err, "Can't handle request, problem with lock file")
		os.Exit(1)
	}

	flagSet := flag.NewFlagSet(cmdName, flag.ExitOnError)
	flagSet.Usage = usage
	err = flagSet.Parse(subArgs())
	if err != nil {
		log.Errorf(cmdCtx, err, "invalid parameters for command: %v", flagSet.Args())
		err = utils.Unlock(file)
		if err != nil {
			log.Error(cmdCtx, err, "file unlock failed")
		}
		os.Exit(1)
	} else {
		go func(ctx context.Context) {
			err = f(ctx, flagSet.Args())
			if err != nil {
				log.Error(ctx, err, "command failed")
			}
			err = utils.Unlock(file)
			if err != nil {
				log.Error(ctx, err, "file unlock failed")
			}
			done <- struct{}{}
		}(cmdCtx)
	}
}
