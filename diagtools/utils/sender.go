package utils

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
)

func SendMultiPart(ctx context.Context, endpoint string, files ...string) (err error) {
	pipeReader, pipeWriter := io.Pipe()
	defer func(pipeReader *io.PipeReader) {
		errClose := pipeReader.Close()
		if errClose != nil {
			log.Error(ctx, errClose, "failed to close pipe reader")
			err = errClose
		}
	}(pipeReader)

	bodyWriter := multipart.NewWriter(pipeWriter)
	log.Info(ctx, "prepare request", "content-type", bodyWriter.FormDataContentType())

	go func() {
		defer func(pipeWriter *io.PipeWriter) {
			errClose := pipeWriter.Close()
			if errClose != nil {
				log.Error(ctx, errClose, "failed to close pipe writer")
				err = errClose
			}
		}(pipeWriter)
		defer func(bodyWriter *multipart.Writer) {
			errClose := bodyWriter.Close()
			if errClose != nil {
				log.Error(ctx, errClose, "failed to close multipart writer")
				err = errClose
			}
		}(bodyWriter)

		for _, fileName := range files {
			func() {
				var part io.Writer
				part, err = bodyWriter.CreateFormFile("file", filepath.Base(fileName))
				if err != nil {
					return
				}

				err = CopyFile(ctx, fileName, part)
				if err != nil {
					log.Error(ctx, err, "failed to copy file", "file", fileName)
					return
				}
			}()
		}
	}()

	var request *http.Request
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, endpoint, pipeReader)
	if err != nil {
		log.Error(ctx, err, "failed to create http request")
		return
	}
	request.Header.Add("Content-Type", bodyWriter.FormDataContentType())

	fName := fmt.Sprintf("%d files", len(files))
	//fName := strings.Join(files, ";")
	err = SendFileRequest(ctx, fName, request)
	return

}

func SendSingleFile(ctx context.Context, targetUrl, fileName string) (err error) {
	startTime := time.Now()

	pipeReader, pipeWriter := io.Pipe()

	defer func(pipeReader *io.PipeReader) {
		errClose := pipeReader.Close()
		if errClose != nil {
			log.Error(ctx, errClose, "failed to close pipe reader")
			err = errClose
		}
	}(pipeReader)

	go func() {
		defer func(pipeWriter *io.PipeWriter) {
			errClose := pipeWriter.Close()
			if errClose != nil {
				log.Error(ctx, errClose, "failed to close pipe writer")
				err = errClose
			}
		}(pipeWriter)

		err = CopyFile(ctx, fileName, pipeWriter)
		if err != nil {
			log.Error(ctx, err, "failed to copy file", "file", fileName)
			return
		}
	}()

	request, err := http.NewRequestWithContext(ctx, http.MethodPut, targetUrl, pipeReader)
	if err != nil {
		log.Error(ctx, err, "failed to create http request")
		return err
	}
	request.Header.Add("Content-Type", "application/octet-stream")

	err = SendFileRequest(ctx, fileName, request)
	if err == nil {
		log.Info(ctx, fmt.Sprintf("uploaded %s", fileName), "duration", time.Since(startTime))
	}
	return err
}

func SendFileRequest(ctx context.Context, fileName string, request *http.Request) (err error) {
	startTime := time.Now()
	log.Infof(ctx, "sending file: %s %s", request.Method, request.URL)

	if log.IsDebugEnabled(ctx) {
		reqBytes, e := httputil.DumpRequestOut(request, false)
		if e != nil {
			log.Error(ctx, e, "failed to dump request out")
		}
		log.Debugf(ctx, "request out: %s", string(reqBytes))
	}

	var response *http.Response
	client, err := FileClient(ctx)
	if err != nil {
		log.Error(ctx, err, "failed to initialize http client")
		return err
	}
	response, err = client.Do(request)
	if err != nil {
		log.Error(ctx, err, "failed to send request")
		return err
	}

	defer func(Body io.ReadCloser) {
		errClose := Body.Close()
		if errClose != nil {
			log.Error(ctx, errClose, "failed to close response body")
			err = errClose
		}
	}(response.Body)

	log.Info(ctx, "Got response: file was sent", "code", response.StatusCode,
		"status", http.StatusText(response.StatusCode), "duration", time.Since(startTime))

	if log.IsDebugEnabled(ctx) {
		respBytes, e := httputil.DumpResponse(response, true)
		if e != nil {
			log.Error(ctx, e, "failed to dump response body")
		} else {
			log.Debugf(ctx, "Get response: %s", string(respBytes))
		}
	}

	if err = checkStatus(ctx, response, fileName); err != nil {
		return err
	}
	return
}

func FileSize(ctx context.Context, fPath string) (size int64, err error) {
	var fileInfo os.FileInfo
	fileInfo, err = os.Stat(fPath)
	if err != nil {
		log.Errorf(ctx, err, "failed to get info for file %s", fPath)
		return 0, err
	}
	return fileInfo.Size(), nil
}

func CopyFile(ctx context.Context, fPath string, writer io.Writer) (err error) {
	startTime := time.Now()

	var fileSize int64
	fileSize, err = FileSize(ctx, fPath)
	if err != nil {
		return err
	}
	log.Infof(ctx, "file '%s' size: %d bytes", fPath, fileSize)

	var file *os.File
	file, err = os.Open(fPath)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		errClose := file.Close()
		if errClose != nil {
			log.Error(ctx, errClose, "failed to close file", "file", fPath)
			err = errClose
		}
	}(file)

	log.Infof(ctx, "start copying file %s", fPath)
	var written int64
	if written, err = io.Copy(writer, file); err != nil {
		log.Error(ctx, err, "failed to copy file content to writer", "file", fPath)
		return err
	}

	latency := time.Since(startTime)
	rate := fmt.Sprintf("%.1f mb/s", float64(written)/latency.Seconds()/1024/1024)
	if latency < time.Second {
		rate = "?"
	}
	log.Info(ctx, "bytes were sent",
		"sent", written, "bytes", fileSize, "rate", rate, "duration", latency)
	return
}

func checkStatus(ctx context.Context, response *http.Response, fileName string) error {
	if response.StatusCode == http.StatusOK {
		return nil // collector: OK
	}
	if response.StatusCode == http.StatusCreated {
		return nil // WebDAV: 201 file created
	}
	if response.StatusCode == http.StatusNoContent {
		log.Info(ctx, "Got 204 [No content]: may be already exists in static-service", "file", fileName)
		return nil // WebDAV: 204 file hasn't changed (cached?)
	}
	log.Error(ctx, nil, fmt.Sprintf("failed to send %s", fileName),
		"code", response.StatusCode, "status", http.StatusText(response.StatusCode))
	return fmt.Errorf("HTTP error while sending %s", fileName)
}

func FileClient(ctx context.Context) (*http.Client, error) {
	client, err := HttpClient()
	if err != nil {
		log.Error(ctx, err, "err While creating http client with TLS configuration")
		return nil, err
	}
	client.Timeout = time.Minute * 5
	return client, nil
}
