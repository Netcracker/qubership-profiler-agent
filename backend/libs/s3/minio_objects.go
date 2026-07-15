package s3

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/minio/minio-go/v7"
)

func (m *MinioClient) ListObjects(ctx context.Context) ([]*minio.ObjectInfo, error) {
	startTime := time.Now()
	bucketName := m.Bucket()

	var objects []*minio.ObjectInfo

	opts := minio.ListObjectsOptions{Recursive: true}
	objectCh := m.Client.ListObjects(ctx, m.Params.BucketName, opts)

	for object := range objectCh {
		if object.Err != nil {
			log.Error(ctx, object.Err, "[%s] couldn't get the list of objects", bucketName)
			ObserveError(operationTypeList)
			return nil, object.Err
		}
		objects = append(objects, common.Ref(object))
	}

	ts := time.Since(startTime)
	// A routine successful LIST runs on every maintain/janitor pass (every
	// minute or faster on a busy stand) — DEBUG keeps steady-state volume
	// from burying real anomalies (PR 708 review #27); ObserveOperation still
	// carries it as a metric for anyone watching LIST cost over time.
	log.Debug(ctx, "[%s] Got list of %d object in %v", bucketName, len(objects), ts)
	ObserveOperation(ts.Seconds(), len(objects), operationTypeList)

	return objects, nil
}

func (m *MinioClient) ListObjectsWithPrefix(ctx context.Context, prefix string) ([]*minio.ObjectInfo, error) {
	startTime := time.Now()
	bucketName := m.Bucket()

	var objects []*minio.ObjectInfo

	opts := minio.ListObjectsOptions{Recursive: true, Prefix: prefix}
	objectCh := m.Client.ListObjects(ctx, m.Params.BucketName, opts)

	for object := range objectCh {
		if object.Err != nil {
			log.Error(ctx, object.Err, "[%s] couldn't get the list of objects with prefix %s", bucketName, prefix)
			ObserveError(operationTypeList)
			return nil, object.Err
		}
		objects = append(objects, common.Ref(object))
	}

	ts := time.Since(startTime)
	// See ListObjects: routine per-pass LIST calls stay at DEBUG (PR 708
	// review #27).
	log.Debug(ctx, "[%s] Got list of %d object with prefix %s in %v", bucketName, len(objects), prefix, ts)
	ObserveOperation(ts.Seconds(), len(objects), operationTypeList)

	return objects, nil
}

func (m *MinioClient) GetObject(ctx context.Context, objectName, localPath string) error {
	startTime := time.Now()
	bucketName := m.Bucket()

	opts := minio.GetObjectOptions{}
	object, err := m.Client.GetObject(ctx, bucketName, objectName, opts)
	if err != nil {
		log.Error(ctx, err, "[%s] couldn't get object [%s]", bucketName, objectName)
		ObserveError(operationTypeGet)
		return err
	}
	defer object.Close()

	if err := os.MkdirAll(localPath, 0700); err != nil && !os.IsExist(err) {
		log.Error(ctx, err, "[%s] couldn't create local directory '%s'", bucketName, localPath)
		ObserveError(operationTypeGet)
		return err
	}

	localFile, err := os.Create(fmt.Sprintf("%s/%s", localPath, objectName))
	if err != nil {
		log.Error(ctx, err, "[%s] couldn't open local file '%s' for object [%s/%s]", bucketName, localFile, objectName)
		ObserveError(operationTypeGet)
		return err
	}
	defer localFile.Close()

	if _, err = io.Copy(localFile, object); err != nil {
		log.Error(ctx, err, "[%s] couldn't save local file '%s' for object [%s/%s]", bucketName, localFile, objectName)
		ObserveError(operationTypeGet)
		return err
	}

	fstat, err := localFile.Stat()
	if err != nil {
		log.Error(ctx, err, "[%s] couldn't get stat for local file '%s'", bucketName, localFile)
		ObserveError(operationTypeGet)
		return err
	}

	ts := time.Since(startTime)
	log.Info(ctx, "[%s] Successfully downloaded object [%s] to %s (%d Mb) in %v",
		bucketName, objectName, localFile, fstat.Size(), ts)
	ObserveOperation(ts.Seconds(), 1, operationTypeGet)
	return nil
}

func (m *MinioClient) PutObject(ctx context.Context, filename, objectName string) (*minio.UploadInfo, error) {
	startTime := time.Now()
	bucketName := m.Bucket()

	object, err := os.Open(filename)
	if err != nil {
		log.Error(ctx, err, "[%s] couldn't open file '%s'", bucketName, filename)
		ObserveError(operationTypePut)
		return nil, err
	}
	defer object.Close()

	objectStat, err := object.Stat()
	if err != nil {
		log.Error(ctx, err, "[%s] couldn't get stat for file '%s'", bucketName, filename)
		ObserveError(operationTypePut)
		return nil, err
	}

	opts := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	info, err := m.Client.PutObject(ctx, bucketName, objectName, object, objectStat.Size(), opts)
	if err != nil {
		log.Error(ctx, err, "[%s] couldn't upload file '%s' [%d bytes] as object '%s'",
			bucketName, filename, objectStat.Size(), objectName)
		ObserveError(operationTypePut)
		return nil, err
	}

	mBytes := info.Size / 1024 / 1024
	ts := time.Since(startTime)
	log.Info(ctx, "[%s] Successfully uploaded '%s' (%d Mb) in %v",
		bucketName, objectName, mBytes, ts)
	ObserveOperation(ts.Seconds(), 1, operationTypePut)
	return &info, nil
}

func (m *MinioClient) RemoveObject(ctx context.Context, objectName, versionId string) error {
	startTime := time.Now()
	bucketName := m.Bucket()

	opts := minio.RemoveObjectOptions{
		GovernanceBypass: true,
		VersionID:        versionId,
	}
	err := m.Client.RemoveObject(ctx, bucketName, objectName, opts)
	if err != nil {
		log.Error(ctx, err, "[%s] couldn't remove object [%s]", bucketName, objectName)
		ObserveError(operationTypeRemove)
		return err
	}

	ts := time.Since(startTime)
	log.Info(ctx, "[%s] Successfully removed object [%s] in %v", bucketName, objectName, ts)
	ObserveOperation(ts.Seconds(), 1, operationTypeRemove)
	return nil
}

func (m *MinioClient) RemoveObjects(ctx context.Context, objList []*minio.ObjectInfo) map[string]error {
	startTime := time.Now()
	bucketName := m.Bucket()
	var errs = make(map[string]error)

	ch := make(chan minio.ObjectInfo)
	go func() {
		for _, obj := range objList {
			ch <- *obj
		}
		close(ch)
	}()
	removeObjCh := m.Client.RemoveObjects(ctx, bucketName, ch, minio.RemoveObjectsOptions{GovernanceBypass: true})

	successfulObjs := len(objList)
	for obj := range removeObjCh {
		if obj.Err != nil {
			log.Error(ctx, obj.Err, "[%s] couldn't remove object [%s]", bucketName, obj.ObjectName)
			ObserveError(operationTypeRemoveMany)
			errs[obj.ObjectName] = obj.Err
		} else {
			successfulObjs--
		}
	}

	ts := time.Since(startTime)
	log.Info(ctx, "[%s] Successfully removed %d objects in %v", bucketName, successfulObjs, ts)
	ObserveOperation(ts.Seconds(), successfulObjs, operationTypeRemoveMany)
	return errs
}
