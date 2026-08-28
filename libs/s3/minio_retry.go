package s3

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/minio/minio-go/v7"
)

// RetryConfig bounds the startup connect retry (PR 708 review #22): the S3
// endpoint racing this process's own startup (MinIO still binding its port,
// DNS not yet resolvable) is routine, not a configuration error, and should
// not crash-loop the pod. A genuine configuration error — bad credentials, a
// bad bucket name, a malformed endpoint — fails on the first attempt.
type RetryConfig struct {
	Attempts  int
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func (c RetryConfig) withDefaults() RetryConfig {
	if c.Attempts <= 0 {
		c.Attempts = 30
	}
	if c.BaseDelay <= 0 {
		c.BaseDelay = time.Second
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = 30 * time.Second
	}
	return c
}

// NewClientWithRetry retries NewClient with exponential backoff while the
// failure looks transient (connection refused, DNS not resolvable yet, a 5xx
// or throttling response). Any other error — including param validation and
// a 4xx other than 408/429 — returns immediately, since retrying cannot fix
// it. onRetry, if set, runs before each wait (e.g. to keep a readiness gate
// in the loading state and log the stage).
func NewClientWithRetry(
	ctx context.Context,
	s3Params Params,
	cfg RetryConfig,
	onRetry func(attempt int, nextDelay time.Duration, err error),
) (*MinioClient, error) {
	return retryConnect(ctx, cfg, onRetry, func() (*MinioClient, error) { return NewClient(ctx, s3Params) })
}

// retryConnect holds the backoff loop itself, factored out so tests can
// inject a fake connect func instead of a live NewClient call.
func retryConnect(
	ctx context.Context,
	cfg RetryConfig,
	onRetry func(attempt int, nextDelay time.Duration, err error),
	connect func() (*MinioClient, error),
) (*MinioClient, error) {
	cfg = cfg.withDefaults()
	delay := cfg.BaseDelay
	var lastErr error
	for attempt := 1; attempt <= cfg.Attempts; attempt++ {
		mc, err := connect()
		if err == nil {
			return mc, nil
		}
		if !isTransientConnectError(err) {
			return nil, err
		}
		lastErr = err
		if attempt == cfg.Attempts {
			break
		}
		if onRetry != nil {
			onRetry(attempt, delay, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
	}
	return nil, lastErr
}

// isTransientConnectError mirrors the uploader's classifyS3Error (§6.2): a
// real HTTP response in the 4xx range (other than 408/429) means the request
// reached S3 and was rejected for a reason retrying will not fix. No HTTP
// response at all — a net.Error, wrapping connection-refused or a DNS lookup
// failure — is exactly the "S3 is not up yet" case this retry exists for.
func isTransientConnectError(err error) bool {
	resp := minio.ToErrorResponse(err)
	if resp.StatusCode != 0 {
		return resp.StatusCode >= 500 || resp.StatusCode == 408 || resp.StatusCode == 429
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// LogRetry is the common onRetry callback: a warning naming the attempt and
// wait, so a slow-starting S3 shows up in logs without looking like a crash.
func LogRetry(ctx context.Context, label string) func(attempt int, nextDelay time.Duration, err error) {
	return func(attempt int, nextDelay time.Duration, err error) {
		log.Warning(ctx, "%s: attempt %d failed, retrying in %v: %v", label, attempt, nextDelay, err)
	}
}
