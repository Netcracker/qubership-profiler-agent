package collector

import (
	"bytes"
	"context"
	"io"
	"os"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/minio/minio-go/v7"
	"github.com/pkg/errors"
)

// S3ObjectStore adapts libs/s3's MinIO client to the hot store's upload
// interface with the 01-write-contract.md §6.2 PUT semantics: Content-MD5 on
// every object and 4xx rejections wrapped in PermanentUploadError so the
// uploader quarantines them instead of retrying (§8).
type S3ObjectStore struct {
	mc *s3.MinioClient
}

// NewS3ObjectStore wraps a connected MinIO client.
func NewS3ObjectStore(mc *s3.MinioClient) *S3ObjectStore {
	return &S3ObjectStore{mc: mc}
}

func (o *S3ObjectStore) PutFile(ctx context.Context, key, localPath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return errors.Wrap(err, "open sealed parquet")
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return errors.Wrap(err, "stat sealed parquet")
	}
	return o.put(ctx, key, f, info.Size(), "application/octet-stream")
}

func (o *S3ObjectStore) PutBytes(ctx context.Context, key string, body []byte) error {
	return o.put(ctx, key, bytes.NewReader(body), int64(len(body)), "application/json")
}

func (o *S3ObjectStore) put(ctx context.Context, key string, body io.Reader, size int64, contentType string) error {
	startTime := time.Now()
	_, err := o.mc.Client.PutObject(ctx, o.mc.Bucket(), key, body, size, minio.PutObjectOptions{
		ContentType:    contentType,
		SendContentMd5: true, // §6.2 step 3
	})
	if err != nil {
		return classifyS3Error(err)
	}
	s3.ObserveOperation(time.Since(startTime).Seconds(), 1, "put")
	return nil
}

// classifyS3Error separates rejections a retry cannot fix (4xx other than
// request-timeout and throttling) from transient failures (5xx, network).
func classifyS3Error(err error) error {
	resp := minio.ToErrorResponse(err)
	if resp.StatusCode >= 400 && resp.StatusCode < 500 &&
		resp.StatusCode != 408 && resp.StatusCode != 429 {
		return &hotstore.PermanentUploadError{Err: err}
	}
	return err
}

var _ hotstore.ObjectStore = (*S3ObjectStore)(nil)
