package cron

import (
	"context"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/robfig/cron/v3"
)

func NewCron(ctx context.Context) *cron.Cron {
	log := &cronLogger{
		errLog: cron.PrintfLogger(logFunc(func(msg string, args ...interface{}) {
			log.Error(ctx, nil, msg, args...)
		})),
		infoLog: cron.PrintfLogger(logFunc(func(msg string, args ...interface{}) {
			log.Info(ctx, msg, args...)
		})),
	}
	return cron.New(
		cron.WithLogger(log),
		cron.WithChain(cron.Recover(log)),
	)

}

type logFunc func(msg string, args ...interface{})

func (f logFunc) Printf(msg string, args ...interface{}) {
	f("[cron] "+msg, args...)
}

type cronLogger struct {
	errLog  cron.Logger
	infoLog cron.Logger
}

func (c *cronLogger) Info(msg string, keysAndValues ...interface{}) {
	c.infoLog.Info(msg, keysAndValues...)
}

func (c *cronLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	c.errLog.Error(err, msg, keysAndValues...)
}
