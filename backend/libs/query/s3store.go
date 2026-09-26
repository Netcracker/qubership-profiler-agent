package query

import (
	"context"
	"io"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/minio/minio-go/v7"
	"github.com/pkg/errors"
)

// S3ObjectReader adapts libs/s3's MinIO client to the cold tier's read
// surface: prefix LISTs for discovery (02 §5.1), ranged random access for
// projected parquet scans, and whole-object GETs for the pods/v1 manifests
// (§2.7). A key deleted between the LIST and the read maps to
// cold.ErrNotFound so discovery treats it as empty (§5.1). The deployment's
// S3_PATH_PREFIX is applied here, at the store boundary: LISTed keys come
// back bucket-root-relative, so ParseKey and the rest of the read path never
// see the prefix.
type S3ObjectReader struct {
	mc     *s3.MinioClient
	prefix s3.KeyPrefix
}

// NewS3ObjectReader wraps a connected MinIO client. pathPrefix is the raw
// S3_PATH_PREFIX value; empty keeps the keys at the bucket root.
func NewS3ObjectReader(mc *s3.MinioClient, pathPrefix string) *S3ObjectReader {
	return &S3ObjectReader{mc: mc, prefix: s3.NewKeyPrefix(pathPrefix)}
}

var _ cold.ObjectStore = (*S3ObjectReader)(nil)

func (r *S3ObjectReader) List(ctx context.Context, prefix string) ([]cold.ObjectInfo, error) {
	objects, err := r.mc.ListObjectsWithPrefix(ctx, r.prefix.Apply(prefix))
	if err != nil {
		return nil, err
	}
	out := make([]cold.ObjectInfo, 0, len(objects))
	for _, obj := range objects {
		key, ok := r.prefix.Strip(obj.Key)
		if !ok {
			continue // cannot happen under an applied prefix; skip defensively
		}
		out = append(out, cold.ObjectInfo{Key: key, Size: obj.Size})
	}
	return out, nil
}

func (r *S3ObjectReader) Open(ctx context.Context, key string) (cold.Object, error) {
	start := time.Now()
	obj, err := r.mc.Client.GetObject(ctx, r.mc.Bucket(), r.prefix.Apply(key), minio.GetObjectOptions{})
	if err != nil {
		return nil, observeGet(ctx, start, err)
	}
	// GetObject is lazy; Stat is the first round trip and surfaces a 404 of a
	// listed-then-compacted key here (02 §5.1).
	stat, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		return nil, observeGet(ctx, start, err)
	}
	s3.ObserveResult(ctx, s3.OperationGet, start, 1, nil)
	return &s3Object{obj: obj, reader: s3.NewObservedReaderAt(ctx, obj, stat.Size), size: stat.Size}, nil
}

func (r *S3ObjectReader) Get(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()
	obj, err := r.mc.Client.GetObject(ctx, r.mc.Bucket(), r.prefix.Apply(key), minio.GetObjectOptions{})
	if err != nil {
		return nil, observeGet(ctx, start, err)
	}
	defer func() { _ = obj.Close() }()
	body, err := io.ReadAll(obj)
	if err != nil {
		return nil, observeGet(ctx, start, err)
	}
	s3.ObserveResult(ctx, s3.OperationGet, start, 1, nil)
	return body, nil
}

// observeGet records a failed GET that started at start and returns err,
// mapped to cold.ErrNotFound for a missing key. A missing key is a completed
// request that read no object, not an S3 failure.
func observeGet(ctx context.Context, start time.Time, err error) error {
	if s3.IsNotFound(err) {
		s3.ObserveResult(ctx, s3.OperationGet, start, 0, nil)
		return errors.Wrap(cold.ErrNotFound, err.Error())
	}
	s3.ObserveResult(ctx, s3.OperationGet, start, 0, err)
	return err
}

// s3Object exposes one S3 object as a sized io.ReaderAt; *minio.Object
// serves discontiguous ReadAt offsets with ranged requests.
type s3Object struct {
	obj    *minio.Object
	reader *s3.ObservedReaderAt
	size   int64
}

func (o *s3Object) ReadAt(p []byte, off int64) (int, error) { return o.reader.ReadAt(p, off) }
func (o *s3Object) Size() int64                             { return o.size }

func (o *s3Object) Close() error {
	o.reader.Close()
	return o.obj.Close()
}
