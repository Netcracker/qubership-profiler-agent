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

	// Thresholds for re-uploading the active gc.log
	gcLogMinGrowthBytes = 10 * 1024 // 10 KB
	gcLogMaxUploadAge   = 10 * time.Minute
)

type GcLogAction struct {
	Action
	// State for full-file active log upload (inode + fingerprint tracking)
	activeInode       inodeID
	activeFingerprint []byte // first fingerprintSize bytes of the file
	activeBytesSent   int64
	activeTargetUrl   string    // URL of the last uploaded gc.log (for overwrite / delete)
	lastUploadTime    time.Time // when we last uploaded the active gc.log
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

// CollectGcLogs uploads rotated GC log files and the active GC log.
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

// uploadActiveLog uploads the full active gc.log file.
//
// Strategy:
//   - First time: PUT full file, remember URL
//   - Subsequent ticks: if file grew by ≥10KB or ≥10min since last upload,
//     PUT full file to the SAME URL (overwrite)
//   - On rotation (inode change, truncate, fingerprint change):
//     PUT full file to a NEW URL (new timestamp), DELETE old URL
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
	defer func() { _ = file.Close() }()

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

	rotated := false
	switch {
	case firstTime:
		// First upload
	case !sameInode:
		log.Infof(fileCtx, "active GC log inode changed (%d -> %d), rotation detected",
			action.activeInode, currentInode)
		rotated = true
	case fileSize < action.activeBytesSent:
		log.Infof(fileCtx, "active GC log was truncated (size=%d, sent=%d)",
			fileSize, action.activeBytesSent)
		rotated = true
	case !sameContent:
		log.Infof(fileCtx, "active GC log fingerprint changed (inode=%d), copy-truncate + regrow detected",
			currentInode)
		rotated = true
	case fileSize == action.activeBytesSent:
		// No new data
		log.Debugf(fileCtx, "no new data in active GC log (size=%d)", fileSize)
		return
	default:
		// File grew — check thresholds before re-uploading
		growth := fileSize - action.activeBytesSent
		age := time.Since(action.lastUploadTime)
		if growth < gcLogMinGrowthBytes && age < gcLogMaxUploadAge {
			log.Debugf(fileCtx, "active GC log grew by %d bytes (%v since last upload), below thresholds",
				growth, age.Truncate(time.Second))
			return
		}
	}

	// On rotation, delete the old file from WebDAV and use a new URL
	oldTargetUrl := action.activeTargetUrl
	if rotated && oldTargetUrl != "" {
		if err := action.getActiveLogTargetUrl(fileCtx); err != nil {
			log.Errorf(fileCtx, err, "failed to get new target URL for active GC log")
			return
		}
		// Delete old file after successful upload (deferred below)
	} else if action.activeTargetUrl == "" {
		// First upload — generate URL
		if err := action.getActiveLogTargetUrl(fileCtx); err != nil {
			log.Errorf(fileCtx, err, "failed to get target URL for active GC log")
			return
		}
	}
	// else: reuse existing activeTargetUrl (overwrite)

	if err := utils.SendSingleFile(fileCtx, action.activeTargetUrl, activeLogPath); err != nil {
		log.Errorf(fileCtx, err, "failed to upload active GC log")
		return
	}

	log.Infof(fileCtx, "uploaded active GC log: size=%d, rotated=%v", fileSize, rotated)

	// Delete old URL after successful upload of new one.
	// Skip DELETE if old and new URLs are the same (can happen when rotation
	// occurs within the same second, since timestamps have second precision).
	if rotated && oldTargetUrl != "" && oldTargetUrl != action.activeTargetUrl {
		if err := utils.DeleteRemoteFile(fileCtx, oldTargetUrl); err != nil {
			log.Errorf(fileCtx, err, "failed to delete old GC log at %s", oldTargetUrl)
			// Non-fatal: we uploaded the new file successfully
		}
	}

	// Update state
	action.activeInode = currentInode
	action.activeFingerprint = currentFingerprint
	action.activeBytesSent = fileSize
	action.lastUploadTime = time.Now()
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
	action.activeTargetUrl = action.TargetUrl
	action.DumpPath = savedDump
	action.ZipDumpPath = savedZip
	return err
}
