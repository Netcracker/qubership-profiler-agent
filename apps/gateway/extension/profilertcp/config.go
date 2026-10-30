package profilertcp

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/units"
)

// Config is the profiler_tcp extension config. The defaults and the
// reasoning behind each one are in docs/plans/2026-10-09-otel-gateway.md §5.
type Config struct {
	// Endpoint is the listen address.
	Endpoint string `mapstructure:"endpoint"`
	// Upstream is the collector's host:port. Required unless Mode is discard
	// and no rule selects passthrough.
	Upstream string `mapstructure:"upstream"`
	// Mode applies when no rule matches.
	Mode rules.Mode `mapstructure:"mode"`
	// ConnectTimeout bounds the upstream dial. The dial and the upstream's
	// handshake reply must fit in the agent's 30-second read timeout.
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
	// MaxConnections caps agent connections. Over the cap a connection is
	// closed on arrival, and the agent retries in 10 seconds. Keep it below
	// half the pod's open-file limit: each passthrough connection takes two
	// descriptors.
	MaxConnections int `mapstructure:"max_connections"`
	// CopyBufferSize is the splice buffer per direction in passthrough.
	CopyBufferSize units.ByteSize `mapstructure:"copy_buffer_size"`
	// HandshakeTimeout bounds the wait for the agent's handshake, which
	// names the source the rules match on.
	HandshakeTimeout time.Duration `mapstructure:"handshake_timeout"`
	// Discard holds the INIT_STREAM_V2 reply fields for discarded agents.
	Discard DiscardConfig `mapstructure:"discard"`
	// Rules are per-source overrides, evaluated after the rules file.
	Rules rules.Set `mapstructure:"rules"`
	// RulesFile is a rules file watched for changes.
	RulesFile string `mapstructure:"rules_file"`

	// prevent unkeyed literal initialization
	_ struct{}
}

// DiscardConfig is what a discarding connection tells the agent about
// stream rotation. The agent rotates as told even when the data is dropped,
// so the defaults are the collector's.
type DiscardConfig struct {
	// RotationPeriod asks the agent to rotate each stream this often; zero
	// disables time-based rotation.
	RotationPeriod time.Duration `mapstructure:"rotation_period"`
	// RequiredRotationSize asks the agent to rotate a stream past this size.
	RequiredRotationSize units.ByteSize `mapstructure:"required_rotation_size"`

	// prevent unkeyed literal initialization
	_ struct{}
}

func createDefaultConfig() *Config {
	return &Config{
		Endpoint:         "0.0.0.0:1715",
		Mode:             rules.Passthrough,
		ConnectTimeout:   time.Second,
		MaxConnections:   256,
		CopyBufferSize:   32 << 10,
		HandshakeTimeout: 10 * time.Second,
		Discard:          DiscardConfig{RequiredRotationSize: 4 << 20},
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
	} else if host, port, err := net.SplitHostPort(c.Upstream); err != nil || host == "" || port == "" {
		errs = append(errs, fmt.Errorf("upstream %q must be host:port", c.Upstream))
	}
	if c.ConnectTimeout <= 0 {
		errs = append(errs, fmt.Errorf("connect_timeout must be positive, got %s", c.ConnectTimeout))
	}
	if c.HandshakeTimeout <= 0 {
		errs = append(errs, fmt.Errorf("handshake_timeout must be positive, got %s", c.HandshakeTimeout))
	}
	if c.Discard.RotationPeriod < 0 {
		errs = append(errs, fmt.Errorf("discard.rotation_period must not be negative, got %s", c.Discard.RotationPeriod))
	}
	if c.Discard.RequiredRotationSize <= 0 {
		errs = append(errs, fmt.Errorf("discard.required_rotation_size must be positive, got %s", c.Discard.RequiredRotationSize))
	}
	if c.MaxConnections <= 0 {
		errs = append(errs, fmt.Errorf("max_connections must be positive, got %d", c.MaxConnections))
	}
	if c.CopyBufferSize < 1<<10 {
		errs = append(errs, fmt.Errorf("copy_buffer_size must be at least 1KiB, got %s", c.CopyBufferSize))
	}
	if err := c.Rules.Validate(rules.TCP, c.Upstream != ""); err != nil {
		errs = append(errs, fmt.Errorf("rules: %w", err))
	}
	return errors.Join(errs...)
}
