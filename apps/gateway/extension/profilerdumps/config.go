package profilerdumps

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/units"
)

// Config is the profiler_dumps extension config. The defaults and the
// reasoning behind each one are in docs/plans/2026-10-09-otel-gateway.md §4.
type Config struct {
	// Endpoint is the listen address.
	Endpoint string `mapstructure:"endpoint"`
	// Upstream is the dumps collector's base URL. Required unless Mode is
	// discard and no rule selects passthrough.
	Upstream string `mapstructure:"upstream"`
	// Mode applies when no rule matches.
	Mode rules.Mode `mapstructure:"mode"`
	// PathPrefix is the URL prefix diagtools uploads under; other paths get 404.
	PathPrefix string `mapstructure:"path_prefix"`
	// AllowedMethods are the methods served; others get 405.
	AllowedMethods []string `mapstructure:"allowed_methods"`
	// ConnectTimeout bounds the upstream dial.
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
	// ResponseHeaderTimeout bounds the wait for the upstream's answer after
	// the body is sent. Keep it longer than the client's 5-minute deadline.
	ResponseHeaderTimeout time.Duration `mapstructure:"response_header_timeout"`
	// ReadHeaderTimeout bounds the wait for the client's request header.
	// The body has no deadline.
	ReadHeaderTimeout time.Duration `mapstructure:"read_header_timeout"`
	// MaxBodySize caps an upload. Over the cap the upstream request is
	// aborted, the rest is drained, and the client gets 204.
	MaxBodySize units.ByteSize `mapstructure:"max_body_size"`
	// MaxBodySizePerType overrides MaxBodySize per dump type: hprof, td,
	// gc, other.
	MaxBodySizePerType map[string]units.ByteSize `mapstructure:"max_body_size_per_type"`
	// MaxConcurrentUploads caps uploads in flight. Over the cap an upload is
	// discarded with 204, never queued.
	MaxConcurrentUploads int `mapstructure:"max_concurrent_uploads"`
	// Rules are per-source overrides, evaluated after the rules file.
	Rules rules.Set `mapstructure:"rules"`
	// RulesFile is a rules file watched for changes.
	RulesFile string `mapstructure:"rules_file"`
	// DrainTimeout bounds how long shutdown waits for in-flight uploads.
	DrainTimeout time.Duration `mapstructure:"drain_timeout"`

	// prevent unkeyed literal initialization
	_ struct{}
}

func createDefaultConfig() *Config {
	return &Config{
		Endpoint:              "0.0.0.0:8080",
		Mode:                  rules.Passthrough,
		PathPrefix:            "/diagnostic",
		AllowedMethods:        []string{http.MethodPut, http.MethodPost, http.MethodDelete},
		ConnectTimeout:        time.Second,
		ResponseHeaderTimeout: 10 * time.Minute,
		ReadHeaderTimeout:     10 * time.Second,
		MaxBodySize:           10 << 30,
		MaxConcurrentUploads:  16,
		DrainTimeout:          30 * time.Second,
	}
}

// Validate checks the config. The Collector calls it before Start.
func (c *Config) Validate() error {
	var errs []error
	if _, _, err := net.SplitHostPort(c.Endpoint); err != nil {
		errs = append(errs, fmt.Errorf("endpoint %q: %w", c.Endpoint, err))
	}
	if !c.Mode.Valid() {
		errs = append(errs, fmt.Errorf("mode is %q; use %s or %s", c.Mode, rules.Passthrough, rules.Discard))
	}
	if c.Upstream == "" {
		if c.Mode == rules.Passthrough {
			errs = append(errs, errors.New("upstream is required unless mode is discard"))
		}
	} else if _, err := c.upstreamURL(); err != nil {
		errs = append(errs, err)
	}
	if !strings.HasPrefix(c.PathPrefix, "/") {
		errs = append(errs, fmt.Errorf("path_prefix %q must start with /", c.PathPrefix))
	}
	if len(c.AllowedMethods) == 0 {
		errs = append(errs, errors.New("allowed_methods is empty"))
	}
	for name, d := range map[string]time.Duration{
		"connect_timeout":         c.ConnectTimeout,
		"response_header_timeout": c.ResponseHeaderTimeout,
		"read_header_timeout":     c.ReadHeaderTimeout,
		"drain_timeout":           c.DrainTimeout,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive, got %s", name, d))
		}
	}
	if c.MaxBodySize <= 0 {
		errs = append(errs, fmt.Errorf("max_body_size must be positive, got %s", c.MaxBodySize))
	}
	for t, size := range c.MaxBodySizePerType {
		if !slices.Contains(rules.DumpTypes, t) {
			errs = append(errs, fmt.Errorf("max_body_size_per_type: dump type %q is unknown; use one of %v", t, rules.DumpTypes))
		}
		if size <= 0 {
			errs = append(errs, fmt.Errorf("max_body_size_per_type.%s must be positive, got %s", t, size))
		}
	}
	if c.MaxConcurrentUploads <= 0 {
		errs = append(errs, fmt.Errorf("max_concurrent_uploads must be positive, got %d", c.MaxConcurrentUploads))
	}
	if err := c.Rules.Validate(rules.Dumps, c.Upstream != ""); err != nil {
		errs = append(errs, fmt.Errorf("rules: %w", err))
	}
	return errors.Join(errs...)
}

func (c *Config) upstreamURL() (*url.URL, error) {
	u, err := url.Parse(c.Upstream)
	if err != nil {
		return nil, fmt.Errorf("upstream %q: %w", c.Upstream, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("upstream %q must be an http:// or https:// URL with a host", c.Upstream)
	}
	return u, nil
}
