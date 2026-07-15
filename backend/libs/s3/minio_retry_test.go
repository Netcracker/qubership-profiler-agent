package s3

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsTransientConnectError(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		transient bool
	}{
		{"dns not found yet", &net.DNSError{IsNotFound: true}, true},
		{"5xx from S3", minio.ErrorResponse{StatusCode: 503}, true},
		{"408 request timeout", minio.ErrorResponse{StatusCode: 408}, true},
		{"429 throttled", minio.ErrorResponse{StatusCode: 429}, true},
		{"403 forbidden — bad credentials", minio.ErrorResponse{StatusCode: 403}, false},
		{"404 no such bucket", minio.ErrorResponse{StatusCode: 404}, false},
		{"local config error", errors.New("empty backet name"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.transient, isTransientConnectError(c.err))
		})
	}
}

func TestRetryConnect_SucceedsAfterTransientFailures(t *testing.T) {
	attempts := 0
	var retried []int
	mc, err := retryConnect(context.Background(), RetryConfig{BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
		func(attempt int, _ time.Duration, _ error) { retried = append(retried, attempt) },
		func() (*MinioClient, error) {
			attempts++
			if attempts < 3 {
				return nil, &net.DNSError{IsNotFound: true}
			}
			return &MinioClient{}, nil
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, mc)
	assert.Equal(t, 3, attempts)
	assert.Equal(t, []int{1, 2}, retried)
}

func TestRetryConnect_StopsImmediatelyOnPermanentError(t *testing.T) {
	attempts := 0
	_, err := retryConnect(context.Background(), RetryConfig{BaseDelay: time.Millisecond},
		func(int, time.Duration, error) { t.Fatal("must not retry a permanent error") },
		func() (*MinioClient, error) {
			attempts++
			return nil, minio.ErrorResponse{StatusCode: 403}
		},
	)
	require.Error(t, err)
	assert.Equal(t, 1, attempts)
}

func TestRetryConnect_GivesUpAfterConfiguredAttempts(t *testing.T) {
	attempts := 0
	_, err := retryConnect(context.Background(), RetryConfig{Attempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
		nil,
		func() (*MinioClient, error) {
			attempts++
			return nil, &net.DNSError{IsNotFound: true}
		},
	)
	require.Error(t, err)
	assert.Equal(t, 3, attempts)
}

func TestRetryConnect_StopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	_, err := retryConnect(ctx, RetryConfig{BaseDelay: time.Hour},
		func(int, time.Duration, error) { cancel() },
		func() (*MinioClient, error) {
			attempts++
			return nil, &net.DNSError{IsNotFound: true}
		},
	)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, attempts)
}
