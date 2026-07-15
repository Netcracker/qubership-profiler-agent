package query

import (
	"context"
	"io"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/minio/minio-go/v7"
	"github.com/pkg/errors"
)

// S3ObjectReader adapts libs/s3's MinIO client to the cold tier's read
// surface: prefix LISTs for discovery (02 §5.1), ranged random access for
// projected parquet scans, and whole-object GETs for the pods/v1 manifests
// (§2.7). A key deleted between the LIST and the read maps to
// cold.ErrNotFound so discovery treats it as empty (§5.1).
type S3ObjectReader struct {
	mc *s3.MinioClient
}

// NewS3ObjectReader wraps a connected MinIO client.
func NewS3ObjectReader(mc *s3.MinioClient) *S3ObjectReader {
	return &S3ObjectReader{mc: mc}
}

var _ cold.ObjectStore = (*S3ObjectReader)(nil)

func (r *S3ObjectReader) List(ctx context.Context, prefix string) ([]cold.ObjectInfo, error) {
	objects, err := r.mc.ListObjectsWithPrefix(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]cold.ObjectInfo, 0, len(objects))
	for _, obj := range objects {
		out = append(out, cold.ObjectInfo{Key: obj.Key, Size: obj.Size})
	}
	return out, nil
}

func (r *S3ObjectReader) Open(ctx context.Context, key string) (cold.Object, error) {
	obj, err := r.mc.Client.GetObject(ctx, r.mc.Bucket(), key, minio.GetObjectOptions{})
	if err != nil {
		return nil, mapNotFound(err)
	}
	// GetObject is lazy; Stat is the first round trip and surfaces a 404 of a
	// listed-then-compacted key here (02 §5.1).
	stat, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		return nil, mapNotFound(err)
	}
	return &s3Object{obj: obj, size: stat.Size}, nil
}

func (r *S3ObjectReader) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := r.mc.Client.GetObject(ctx, r.mc.Bucket(), key, minio.GetObjectOptions{})
	if err != nil {
		return nil, mapNotFound(err)
	}
	defer func() { _ = obj.Close() }()
	body, err := io.ReadAll(obj)
	if err != nil {
		return nil, mapNotFound(err)
	}
	return body, nil
}

func mapNotFound(err error) error {
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return errors.Wrap(cold.ErrNotFound, err.Error())
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
