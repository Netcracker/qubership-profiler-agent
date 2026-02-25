package actions

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/utils"
)

const (
	gcLogFileName      = "gc.log"
	gcActiveUploadName = "gc.log"
	fingerprintSize    = 64 // bytes to read for content fingerprint
)

type GcLogAction struct {
	Action
	// State for incremental active log upload (inode + fingerprint tracking)
	activeInode       inodeID
	activeFingerprint []byte // first fingerprintSize bytes of the file
	activeBytesSent   int64
}

func CreateGcLogAction(ctx context.Context) (action *GcLogAction, err error) {
	action = &GcLogAction{
		Action: Action{
			DcdEnabled: constants.IsDcdEnabled(),
			CmdTimeout: 10 * time.Second,
		},
	}
	err = action.GetPodName(ctx)
	return action, err
}

// CollectGcLogs uploads rotated GC log files and incrementally uploads the active GC log.
func (action *GcLogAction) CollectGcLogs(ctx context.Context) error {
	gcLogFolder := filepath.Join(constants.LogFolder(), constants.GcLogSubFolder)

	if _, err := os.Stat(gcLogFolder); os.IsNotExist(err) {
		log.Debugf(ctx, "GC log folder %s does not exist, skipping", gcLogFolder)
		return nil
	}

	action.uploadRotatedLogs(ctx, gcLogFolder)
	action.uploadActiveLog(ctx, gcLogFolder)
	return nil
}

// uploadRotatedLogs finds rotated GC log files (gc.log.0, gc.log.1, ...),
// uploads them, and deletes after successful upload.
func (action *GcLogAction) uploadRotatedLogs(ctx context.Context, gcLogFolder string) {
	pattern := filepath.Join(gcLogFolder, gcLogFileName+".*")
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Errorf(ctx, err, "failed to glob rotated GC logs")
		return
	}

	if len(files) == 0 {
		return
	}

	log.Infof(ctx, "found %d rotated GC log files to upload", len(files))

	for _, filePath := range files {
		fileCtx := log.AppendCtx(ctx, "gclog-rotated")

		action.DumpPath = filePath
		action.ZipDumpPath = ""

		if err := action.GetTargetUrl(fileCtx); err != nil {
			log.Errorf(fileCtx, err, "failed to get target URL for %s", filePath)
			continue
		}

		if err := utils.SendSingleFile(fileCtx, action.TargetUrl, filePath); err != nil {
			log.Errorf(fileCtx, err, "failed to upload %s", filePath)
			continue
		}

		if err := os.Remove(filePath); err != nil {
			log.Errorf(fileCtx, err, "failed to delete %s", filePath)
		}
	}
}

// uploadActiveLog incrementally uploads the active gc.log file.
//
// Uses inode + content fingerprint tracking (inspired by file.d):
//   - Different inode           → rename rotation, upload from 0
//   - Same inode, file shrank   → copy-truncate rotation, upload from 0
//   - Same inode, fingerprint changed → copy-truncate + regrow, upload from 0
//   - Same inode, same fingerprint, file grew → upload only the new tail
//   - Same inode, same fingerprint, same size → no new data, skip
func (action *GcLogAction) uploadActiveLog(ctx context.Context, gcLogFolder string) {
	activeLogPath := filepath.Join(gcLogFolder, gcLogFileName)

	file, err := os.Open(activeLogPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Debugf(ctx, "active GC log %s does not exist, skipping", activeLogPath)
		} else {
			log.Errorf(ctx, err, "failed to open active GC log %s", activeLogPath)
		}
		return
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		log.Errorf(ctx, err, "failed to stat active GC log")
		return
	}

	fileSize := info.Size()
	if fileSize == 0 {
		return
	}

	fileCtx := log.AppendCtx(ctx, "gclog-active")
	currentInode := getInode(info)

	// Read fingerprint: first N bytes of the file for content comparison.
	// On subsequent reads, use the same byte count as the first read
	// so that appending to the file doesn't change the fingerprint.
	fpReadSize := fingerprintSize
	if len(action.activeFingerprint) > 0 && len(action.activeFingerprint) < fpReadSize {
		fpReadSize = len(action.activeFingerprint)
	}
	currentFingerprint, err := readFingerprint(file, fpReadSize)
	if err != nil {
		log.Errorf(fileCtx, err, "failed to read fingerprint of active GC log")
		return
	}

	firstTime := action.activeInode == 0
	sameInode := !firstTime && currentInode == action.activeInode
	sameContent := sameInode && bytes.Equal(currentFingerprint, action.activeFingerprint)

	var offset int64
	switch {
	case firstTime || !sameInode:
		// Inode changed → rename rotation (gc.log → gc.log.0, new gc.log created)
		if !firstTime {
			log.Infof(fileCtx, "active GC log inode changed (%d -> %d), rotation detected",
				action.activeInode, currentInode)
		}
		offset = 0
	case fileSize < action.activeBytesSent:
		// Same inode, file shrank → copy-truncate
		log.Infof(fileCtx, "active GC log was truncated (size=%d, sent=%d)",
			fileSize, action.activeBytesSent)
		offset = 0
	case !sameContent:
		// Same inode, same or larger size, but content changed →
		// copy-truncate happened and file already grew back
		log.Infof(fileCtx, "active GC log fingerprint changed (inode=%d), copy-truncate + regrow detected",
			currentInode)
		offset = 0
	case fileSize == action.activeBytesSent:
		// Same file, same content, same size → nothing new
		log.Debugf(fileCtx, "no new data in active GC log (size=%d)", fileSize)
		return
	default:
		// Same file, same content start, file grew → incremental upload
		offset = action.activeBytesSent
	}

	// Write the portion to a temp file for upload
	tmpFile, err := os.CreateTemp(gcLogFolder, ".gc-upload-*.tmp")
	if err != nil {
		log.Errorf(fileCtx, err, "failed to create temp file for active GC log upload")
		return
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		tmpFile.Close()
		log.Errorf(fileCtx, err, "failed to seek in active GC log")
		return
	}

	written, err := io.Copy(tmpFile, file)
	tmpFile.Close()
	if err != nil {
		log.Errorf(fileCtx, err, "failed to copy active GC log to temp file")
		return
	}

	if written == 0 {
		return
	}

	// Override filename in URL to use gc.log, not the tmp name
	if err := action.getActiveLogTargetUrl(fileCtx); err != nil {
		log.Errorf(fileCtx, err, "failed to get target URL for active GC log")
		return
	}

	if err := utils.SendSingleFile(fileCtx, action.TargetUrl, tmpPath); err != nil {
		log.Errorf(fileCtx, err, "failed to upload active GC log")
		return
	}

	log.Infof(fileCtx, "uploaded active GC log: offset=%d, bytes=%d, totalSize=%d", offset, written, fileSize)

	// Update state
	action.activeInode = currentInode
	action.activeFingerprint = currentFingerprint
	action.activeBytesSent = offset + written
}

// readFingerprint reads up to maxLen bytes from the beginning of the file.
func readFingerprint(file *os.File, maxLen int) ([]byte, error) {
	buf := make([]byte, maxLen)
	n, err := io.ReadAtLeast(file, buf, 1)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (action *GcLogAction) getActiveLogTargetUrl(ctx context.Context) error {
	// Use gc.log as the filename in the URL, not the temp file name
	savedDump := action.DumpPath
	savedZip := action.ZipDumpPath
	action.DumpPath = gcActiveUploadName
	action.ZipDumpPath = ""
	err := action.GetTargetUrl(ctx)
	action.DumpPath = savedDump
	action.ZipDumpPath = savedZip
	return err
}
