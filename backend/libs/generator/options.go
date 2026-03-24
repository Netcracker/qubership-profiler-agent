package generator

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
)

type (
	Options struct {
		DataDir       string            `js:"data"`
		CollectorHost string            `js:"host"`
		LogLevel      string            `js:"log"`
		Timeouts      Timeouts          `js:"timeout"`
		Tags          map[string]string `js:"tags"`
		TestDuration  string            `js:"duration"`
		PodCount      int               `js:"pods"`
		Prefixes      Prefixes          `js:"prefix"`
	}

	Timeouts struct {
		Connect string `js:"connect"`
		Session string `js:"session"`
	}

	Prefixes struct {
		Namespace string `js:"namespace"`
		Service   string `js:"service"`
		PodName   string `js:"podName"`
	}
)

func (opts Options) DataDirectory() string {
	return opts.DataDir
}

func (opts Options) Validate() error {
	if opts.DataDir == "" {
		return errors.New("empty path for data")
	}

	if opts.Timeouts.Connect == "" {
		return errors.New("empty connection timeout")
	}
	_, err := time.ParseDuration(opts.Timeouts.Connect)
	if err != nil {
		return errors.Wrapf(err, "invalid connection timeout: %s", opts.Timeouts.Connect)
	}

	if opts.Timeouts.Session == "" {
		return errors.New("empty session timeout")
	}
	_, err = time.ParseDuration(opts.Timeouts.Session)
	if err != nil {
		return errors.Wrapf(err, "invalid session timeout: %s", opts.Timeouts.Session)
	}

	if opts.TestDuration == "" {
		return errors.New("empty test duration")
	}
	_, err = time.ParseDuration(opts.TestDuration)
	if err != nil {
		return errors.Wrapf(err, "invalid test duration: %s", opts.TestDuration)
	}

	if opts.PodCount < 1 {
		return errors.Errorf("invalid pod count: %d", opts.PodCount)
	}

	return err
}

func (opts Options) ConnectTimeout() time.Duration {
	t, _ := time.ParseDuration(opts.Timeouts.Connect)
	return t
}

func (opts Options) SessionTimeout() time.Duration {
	t, _ := time.ParseDuration(opts.Timeouts.Session)
	return t
}

func (opts Options) Duration() time.Duration {
	t, _ := time.ParseDuration(opts.TestDuration)
	return t
}

func (opts Options) ProtocolAddr() string {
	return fmt.Sprintf("%s:1715", opts.CollectorHost)
}

func (p Prefixes) String() string {
	return fmt.Sprintf("{ns:%s svc:%s pod:%s}", p.Namespace, p.Service, p.PodName)
}
