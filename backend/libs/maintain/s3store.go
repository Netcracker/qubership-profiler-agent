package maintain

import (
	"bytes"
	"context"

	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/minio/minio-go/v7"
	"github.com/pkg/errors"
)

// S3ObjectStore adapts libs/s3's MinIO client to the job's read-write
// surface: prefix LISTs with LastModified for the delete-grace clock, ranged
// reads for the parquet merge, Content-MD5 PUTs (01-write-contract.md §6.2),
// and idempotent deletes.
type S3ObjectStore struct {
	mc *s3.MinioClient
}

// NewS3ObjectStore wraps a connected MinIO client.
func NewS3ObjectStore(mc *s3.MinioClient) *S3ObjectStore {
	return &S3ObjectStore{mc: mc}
}

var _ ObjectStore = (*S3ObjectStore)(nil)

func (o *S3ObjectStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	objects, err := o.mc.ListObjectsWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]ObjectInfo, 0, len(objects))
	for _, obj := range objects {
		out = append(out, ObjectInfo{Key: obj.Key, Size: obj.Size, LastModified: obj.LastModified})
	}
	return out, nil
}

func (o *S3ObjectStore) Open(ctx context.Context, key string) (Object, error) {
	obj, err := o.mc.Client.GetObject(ctx, o.mc.Bucket(), key, minio.GetObjectOptions{})
	if err != nil {
		return nil, mapNotFound(err)
	}
	// GetObject is lazy; Stat is the first round trip and surfaces a 404 of a
	// concurrently deleted key here.
	stat, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		return nil, mapNotFound(err)
	}
	return &s3Object{obj: obj, size: stat.Size}, nil
}

func (o *S3ObjectStore) Put(ctx context.Context, key string, body []byte) error {
	_, err := o.mc.Client.PutObject(ctx, o.mc.Bucket(), key,
		bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{
			ContentType:    "application/octet-stream",
			SendContentMd5: true, // 01 §6.2 step 3
		})
	return err
}

func (o *S3ObjectStore) Delete(ctx context.Context, key string) error {
	// S3 DeleteObject succeeds on a missing key, which is exactly the
	// idempotent-delete contract of ObjectStore.
	return o.mc.Client.RemoveObject(ctx, o.mc.Bucket(), key, minio.RemoveObjectOptions{})
}

func mapNotFound(err error) error {
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return errors.Wrap(ErrNotFound, err.Error())
	}
	return err
}

// s3Object exposes one S3 object as a sized io.ReaderAt; *minio.Object
// serves discontiguous ReadAt offsets with ranged requests.
type s3Object struct {
	obj  *minio.Object
	size int64
}

func (o *s3Object) ReadAt(p []byte, off int64) (int, error) { return o.obj.ReadAt(p, off) }
func (o *s3Object) Close() error                            { return o.obj.Close() }
func (o *s3Object) Size() int64                             { return o.size }
