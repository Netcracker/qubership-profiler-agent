package helpers

import (
	"context"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

const (
	bucketName = "integration-test"
	minioImage = "minio/minio"
)

type MinioContainer struct {
	*minio.MinioContainer
	Client *s3.MinioClient
	Params *s3.Params
}

// CreateMinioContainer run a container (and stop tests if could not)
func CreateMinioContainer(ctx context.Context) *MinioContainer {
	image := testcontainers.WithImage(minioImage)
	minioContainer, err := minio.RunContainer(ctx, image)
	if err != nil {
		log.Fatal(ctx, err, "couldn't start minio container (%s)", minioImage)
	}

	url, err := minioContainer.ConnectionString(ctx)
	if err != nil {
		log.Fatal(ctx, err, "invalid connection string: %s", url)
	}
	log.Debug(ctx, "Got connection string from minio container: %s", url)

	minioParams := s3.Params{
		Endpoint:        url,
		AccessKeyID:     minioContainer.Username,
		SecretAccessKey: minioContainer.Password,
		UseSSL:          false,
		InsecureSSL:     false,
		BucketName:      bucketName,
	}
	minioClient, err := s3.NewClient(ctx, minioParams)
	if err != nil {
		log.Fatal(ctx, err, "couldn't create minio client")
	}

	return &MinioContainer{
		MinioContainer: minioContainer,
		Client:         minioClient,
		Params:         &minioParams,
	}
}

func (mc *MinioContainer) Terminate(ctx context.Context) error {
	err := mc.Client.RemoveBucket(ctx, mc.Params.BucketName)
	if err != nil {
		log.Error(ctx, err, "couldn't remove bucket")
	}
	err = (mc.MinioContainer).Terminate(ctx)
	if err != nil {
		log.Error(ctx, err, "error terminating minio container")
	}
	return err
}
