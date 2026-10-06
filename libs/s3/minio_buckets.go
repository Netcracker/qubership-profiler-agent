package s3

import (
	"context"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/minio/minio-go/v7"
)

func (m *MinioClient) Bucket() string {
	return m.Params.BucketName
}

func (m *MinioClient) ListBuckets(ctx context.Context) ([]minio.BucketInfo, error) {
	buckets, err := m.Client.ListBuckets(ctx)
	if err != nil {
		log.Error(ctx, err, "couldn't get the list of buckets")
		return nil, err
	}
	return buckets, nil
}

func (m *MinioClient) MakeBucket(ctx context.Context, bucketName string) error {
	startTime := time.Now()
	options := minio.MakeBucketOptions{Region: m.Params.Region, ObjectLocking: m.Params.ObjectLocking}
	err := m.Client.MakeBucket(ctx, bucketName, options)
	if err != nil {
		errResponse := minio.ToErrorResponse(err)
		if errResponse.Code != "BucketAlreadyOwnedByYou" {
			log.Error(ctx, err, "[%s] couldn't create a bucket", bucketName)
			return err
		} else {
			log.Debug(ctx, "[%s] bucket is already existing", bucketName)
		}
	} else {
		log.Info(ctx, "[%s] Successfully created bucket in %v", bucketName, time.Since(startTime))
	}
	return nil
}

func (m *MinioClient) RemoveBucket(ctx context.Context, bucketName string) error {
	startTime := time.Now()
	err := m.Client.RemoveBucket(ctx, bucketName)
	if err != nil {
		log.Error(ctx, err, "couldn't remove bucket %s", bucketName)
		return err
	}
	log.Info(ctx, "[%s] Successfully removed bucket in %v", bucketName, time.Since(startTime))
	return nil
}
