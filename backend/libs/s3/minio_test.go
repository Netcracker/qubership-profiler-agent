package s3

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tlsParamsWithCAFile returns params that pass IsValid and point CAFile at a
// file holding content. minio.New does not dial, so no S3 endpoint is needed.
func tlsParamsWithCAFile(t *testing.T, content []byte) Params {
	t.Helper()
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	require.NoError(t, os.WriteFile(caFile, content, 0o600))
	return Params{
		Endpoint:        "s3.invalid:9000",
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
		UseSSL:          true,
		BucketName:      "profiler",
		CAFile:          caFile,
	}
}

func selfSignedCertificatePEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestNewReadOnlyClient_RejectsCAFileWithoutPEMCertificate(t *testing.T) {
	params := tlsParamsWithCAFile(t, []byte("not a PEM certificate\n"))

	mc, err := NewReadOnlyClient(context.Background(), params)

	require.Error(t, err)
	assert.Nil(t, mc)
	assert.ErrorContains(t, err, params.CAFile)
	assert.ErrorContains(t, err, "no PEM certificates found")
}

func TestNewReadOnlyClient_AcceptsPEMCertificate(t *testing.T) {
	params := tlsParamsWithCAFile(t, selfSignedCertificatePEM(t))

	mc, err := NewReadOnlyClient(context.Background(), params)

	require.NoError(t, err)
	assert.NotNil(t, mc)
}

// The old code returned a nil client with a nil error here, and NewClient
// dereferenced it in MakeBucket.
func TestNewClient_ReturnsCAParseErrorWithoutPanicking(t *testing.T) {
	params := tlsParamsWithCAFile(t, []byte("not a PEM certificate\n"))

	mc, err := NewClient(context.Background(), params)

	assert.ErrorContains(t, err, "no PEM certificates found")
	assert.Nil(t, mc)
}

func TestNewClientWithRetry_DoesNotRetryCAParseError(t *testing.T) {
	params := tlsParamsWithCAFile(t, []byte("not a PEM certificate\n"))

	mc, err := NewClientWithRetry(context.Background(), params,
		RetryConfig{Attempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond},
		func(attempt int, _ time.Duration, err error) {
			t.Fatalf("retried a CA parse error after attempt %d: %v", attempt, err)
		},
	)

	assert.ErrorContains(t, err, "no PEM certificates found")
	assert.Nil(t, mc)
}
