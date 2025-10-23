package s3

import (
	"context"
	"crypto/x509"
	"os"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioClient struct {
	Client *minio.Client
	Params Params
}

func NewClient(ctx context.Context, s3Params Params) (*MinioClient, error) {

	if err := s3Params.IsValid(); err != nil {
		log.Error(ctx, err, "couldn't connect to S3 storage %s", s3Params.Endpoint)
		return nil, err
	}

	tr, err := minio.DefaultTransport(s3Params.UseSSL)
	if err != nil {
		log.Error(ctx, err, "error creating the default transport layer for minio")
		return nil, err
	}
	if s3Params.UseSSL {
		tr.TLSClientConfig.InsecureSkipVerify = s3Params.InsecureSSL
		if s3Params.CAFile != "" {
			caCert, err := os.ReadFile(s3Params.CAFile)
			if err != nil {
				log.Error(ctx, err, "error loading CA certificate for minio")
				return nil, err
			}
			tr.TLSClientConfig.RootCAs = x509.NewCertPool()
			if ok := tr.TLSClientConfig.RootCAs.AppendCertsFromPEM(caCert); !ok {
				log.Error(ctx, nil, "error parsing the certificate for minio")
				return nil, err
			}
		}
	}
	c, err := minio.New(s3Params.Endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(s3Params.AccessKeyID, s3Params.SecretAccessKey, ""),
		Secure:    s3Params.UseSSL,
		Transport: tr,
	})
	if err != nil {
		log.Error(ctx, err, "couldn't connect to S3 storage %s", s3Params.Endpoint)
		return nil, err
	}

	mc := &MinioClient{
		Client: c,
		Params: s3Params,
	}

	registerMetrics()

	err = mc.MakeBucket(ctx, mc.Params.BucketName)

	return mc, err
}
