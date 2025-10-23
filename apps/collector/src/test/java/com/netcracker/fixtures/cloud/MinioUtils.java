package com.netcracker.fixtures.cloud;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;


import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import java.security.InvalidKeyException;

@ApplicationScoped
public class MinioUtils {

    @Inject
    MinioClient minioClient;
    final String bucketName="profiler";
    final String fileDir = "src/test/resources/persistence/cloud/";
    public void uploadFileAndGetPath( String objectName, String fileName) {

        try {
            String filePath = fileDir + fileName;
            boolean found = minioClient.bucketExists(BucketExistsArgs.builder()
            .bucket(bucketName)
            .build());

            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder()
                        .bucket(bucketName)
                        .build());
            }

            minioClient.uploadObject(UploadObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .filename(filePath)
                    .build());
        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
            Log.error(e.getMessage());
        }
    }
}
