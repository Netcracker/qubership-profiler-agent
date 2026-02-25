package actions

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"
	"github.com/Netcracker/qubership-profiler-agent/diagtools/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testCtx = context.Background()

func init() {
	log.SetupTestLogger()
}

// uploadCapture records bodies received by the test HTTP server.
type uploadCapture struct {
	mu      sync.Mutex
	uploads []capturedUpload
	deletes []string
}

type capturedUpload struct {
	path string
	body []byte
}

func (c *uploadCapture) add(path string, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uploads = append(c.uploads, capturedUpload{path: path, body: body})
}

func (c *uploadCapture) addDelete(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deletes = append(c.deletes, path)
}

func (c *uploadCapture) get() []capturedUpload {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]capturedUpload{}, c.uploads...)
}

func (c *uploadCapture) getDeletes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string{}, c.deletes...)
}

func (c *uploadCapture) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uploads = nil
	c.deletes = nil
}

// setupTestServer creates an httptest.Server that captures PUT and DELETE requests.
func setupTestServer(capture *uploadCapture) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			capture.add(r.URL.Path, body)
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			capture.addDelete(r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
}

// setupTestEnv configures env vars so DiagService / GetTargetUrl point to the test server.
func setupTestEnv(t *testing.T, serverURL string) {
	t.Helper()
	t.Setenv(constants.NcDiagAgentService, serverURL)
	t.Setenv(constants.NcCloudNamespace, "test-ns")
}

// newTestAction creates a GcLogAction with PodName pre-set.
func newTestAction() *GcLogAction {
	return &GcLogAction{
		Action: Action{
			DcdEnabled: true,
			PodName:    "test-pod",
		},
	}
}

// writeFile is a test helper to write content to a file.
func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	require.NoError(t, os.WriteFile(path, data, 0644))
}

// replaceFile removes the old file and creates a new one (new inode).
func replaceFile(t *testing.T, path string, data []byte) {
	t.Helper()
	os.Remove(path)
	writeFile(t, path, data)
}

// --- Tests for uploadRotatedLogs ---

func TestUploadRotatedLogs_UploadsAndDeletes(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	gcDir := t.TempDir()

	writeFile(t, filepath.Join(gcDir, "gc.log.0"), []byte("rotated-log-0"))
	writeFile(t, filepath.Join(gcDir, "gc.log.1"), []byte("rotated-log-1"))
	writeFile(t, filepath.Join(gcDir, "gc.log"), []byte("active-log"))

	action := newTestAction()
	action.uploadRotatedLogs(testCtx, gcDir)

	uploads := capture.get()
	assert.Equal(t, 2, len(uploads), "should upload exactly 2 rotated files")

	_, err := os.Stat(filepath.Join(gcDir, "gc.log.0"))
	assert.True(t, os.IsNotExist(err), "gc.log.0 should be deleted after upload")
	_, err = os.Stat(filepath.Join(gcDir, "gc.log.1"))
	assert.True(t, os.IsNotExist(err), "gc.log.1 should be deleted after upload")

	_, err = os.Stat(filepath.Join(gcDir, "gc.log"))
	assert.NoError(t, err, "gc.log (active) should NOT be deleted")
}

func TestUploadRotatedLogs_NoFiles(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	action := newTestAction()
	action.uploadRotatedLogs(testCtx, t.TempDir())

	assert.Empty(t, capture.get())
}

// --- Tests for uploadActiveLog (full-file strategy) ---

func TestUploadActiveLog_FirstUpload(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	gcDir := t.TempDir()
	content := []byte("first gc log content")
	writeFile(t, filepath.Join(gcDir, "gc.log"), content)

	action := newTestAction()
	action.uploadActiveLog(testCtx, gcDir)

	uploads := capture.get()
	require.Equal(t, 1, len(uploads))
	assert.Equal(t, content, uploads[0].body)
	assert.Equal(t, int64(len(content)), action.activeBytesSent)
	assert.NotEqual(t, inodeID(0), action.activeInode)
	assert.NotEmpty(t, action.activeTargetUrl)
}

func TestUploadActiveLog_SmallGrowthSkipped(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	gcDir := t.TempDir()
	logPath := filepath.Join(gcDir, "gc.log")

	initialContent := []byte("initial gc log data here")
	writeFile(t, logPath, initialContent)

	action := newTestAction()
	action.uploadActiveLog(testCtx, gcDir)
	require.Equal(t, 1, len(capture.get()))
	capture.reset()

	// Append small amount of data (below 10KB threshold)
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("-APPENDED"))
	require.NoError(t, err)
	f.Close()

	action.uploadActiveLog(testCtx, gcDir)

	// Should NOT upload because growth < 10KB and age < 10min
	assert.Empty(t, capture.get(), "small growth should be skipped")
}

func TestUploadActiveLog_LargeGrowthUploaded(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	gcDir := t.TempDir()
	logPath := filepath.Join(gcDir, "gc.log")

	initialContent := []byte("initial gc log data here")
	writeFile(t, logPath, initialContent)

	action := newTestAction()
	action.uploadActiveLog(testCtx, gcDir)
	savedUrl := action.activeTargetUrl
	require.Equal(t, 1, len(capture.get()))
	capture.reset()

	// Append more than 10KB
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	bigAppend := make([]byte, 11*1024)
	for i := range bigAppend {
		bigAppend[i] = 'X'
	}
	_, err = f.Write(bigAppend)
	require.NoError(t, err)
	f.Close()

	action.uploadActiveLog(testCtx, gcDir)

	uploads := capture.get()
	require.Equal(t, 1, len(uploads))
	// Full file should be uploaded (not just the delta)
	expectedSize := len(initialContent) + len(bigAppend)
	assert.Equal(t, expectedSize, len(uploads[0].body), "full file should be uploaded")
	// Same URL (overwrite)
	assert.Equal(t, savedUrl, action.activeTargetUrl, "URL should stay the same on overwrite")
	assert.Empty(t, capture.getDeletes(), "no DELETE on overwrite")
}

func TestUploadActiveLog_TimeThresholdTriggersUpload(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	gcDir := t.TempDir()
	logPath := filepath.Join(gcDir, "gc.log")

	initialContent := []byte("initial gc log data here")
	writeFile(t, logPath, initialContent)

	action := newTestAction()
	action.uploadActiveLog(testCtx, gcDir)
	require.Equal(t, 1, len(capture.get()))
	capture.reset()

	// Append small amount but fake old lastUploadTime
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte("-APPENDED"))
	require.NoError(t, err)
	f.Close()

	action.lastUploadTime = time.Now().Add(-11 * time.Minute)

	action.uploadActiveLog(testCtx, gcDir)

	uploads := capture.get()
	require.Equal(t, 1, len(uploads), "should upload after time threshold exceeded")
}

func TestUploadActiveLog_InodeChangeDetectsRotation(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	gcDir := t.TempDir()
	logPath := filepath.Join(gcDir, "gc.log")

	// First upload
	writeFile(t, logPath, []byte("old log data"))
	action := newTestAction()
	action.uploadActiveLog(testCtx, gcDir)
	oldInode := action.activeInode
	oldUrl := action.activeTargetUrl
	capture.reset()

	// Simulate rename-rotation: delete and create new file (new inode)
	newContent := []byte("new log after rotation")
	replaceFile(t, logPath, newContent)

	action.uploadActiveLog(testCtx, gcDir)

	uploads := capture.get()
	require.Equal(t, 1, len(uploads))
	assert.Equal(t, newContent, uploads[0].body, "after rotation, entire file should be uploaded")
	assert.NotEqual(t, oldInode, action.activeInode, "inode should change after rotation")
	assert.Equal(t, int64(len(newContent)), action.activeBytesSent)
	// A new target URL should be generated (may or may not differ from old if within same second)
	assert.NotEmpty(t, action.activeTargetUrl)
	// Within the same second, old and new URLs are identical → DELETE must be skipped
	// to avoid deleting the just-uploaded file.
	if oldUrl == action.activeTargetUrl {
		assert.Empty(t, capture.getDeletes(), "DELETE should be skipped when old and new URLs are the same")
	} else {
		deletes := capture.getDeletes()
		require.Equal(t, 1, len(deletes), "old URL should be deleted on rotation")
	}
}

func TestUploadActiveLog_RotationSameSecondNoDelete(t *testing.T) {
	// Verifies that when rotation happens within the same second,
	// the old URL is NOT deleted (since it equals the new URL).
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	gcDir := t.TempDir()
	logPath := filepath.Join(gcDir, "gc.log")

	writeFile(t, logPath, []byte("old log data"))
	action := newTestAction()
	action.uploadActiveLog(testCtx, gcDir)
	oldUrl := action.activeTargetUrl
	capture.reset()

	// Simulate rotation immediately (same second → same timestamp URL)
	replaceFile(t, logPath, []byte("new log after rotation"))
	action.uploadActiveLog(testCtx, gcDir)

	// Both URLs use the same second-precision timestamp, so they're identical
	if oldUrl == action.activeTargetUrl {
		assert.Empty(t, capture.getDeletes(),
			"must NOT delete when old URL == new URL (same-second rotation)")
	}
	// File was still uploaded
	require.Equal(t, 1, len(capture.get()))
}

func TestUploadActiveLog_TruncateDetected(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	gcDir := t.TempDir()
	logPath := filepath.Join(gcDir, "gc.log")

	// Write a large file, upload it
	bigContent := make([]byte, 1000)
	for i := range bigContent {
		bigContent[i] = 'X'
	}
	writeFile(t, logPath, bigContent)

	action := newTestAction()
	action.uploadActiveLog(testCtx, gcDir)
	assert.Equal(t, int64(1000), action.activeBytesSent)
	savedInode := action.activeInode
	capture.reset()

	// Truncate by writing smaller content to the SAME file (keeping inode)
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_TRUNC, 0644)
	require.NoError(t, err)
	smallContent := []byte("truncated")
	_, err = f.Write(smallContent)
	require.NoError(t, err)
	f.Close()

	// Verify inode is preserved (truncate keeps inode)
	info, err := os.Stat(logPath)
	require.NoError(t, err)
	assert.Equal(t, savedInode, getInode(info), "inode should be preserved after truncate")

	action.uploadActiveLog(testCtx, gcDir)

	uploads := capture.get()
	require.Equal(t, 1, len(uploads))
	assert.Equal(t, smallContent, uploads[0].body, "after truncation, entire file should be re-uploaded")
	assert.Equal(t, int64(len(smallContent)), action.activeBytesSent)
	assert.NotEmpty(t, action.activeTargetUrl)
}

func TestUploadActiveLog_CopyTruncateAndRegrow(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	gcDir := t.TempDir()
	logPath := filepath.Join(gcDir, "gc.log")

	// Write initial content and upload
	initialContent := make([]byte, 500)
	for i := range initialContent {
		initialContent[i] = 'A'
	}
	writeFile(t, logPath, initialContent)

	action := newTestAction()
	action.uploadActiveLog(testCtx, gcDir)
	assert.Equal(t, int64(500), action.activeBytesSent)
	savedInode := action.activeInode
	capture.reset()

	// Simulate copy-truncate + regrow: truncate file in-place (same inode),
	// then write NEW content that is LARGER than previous size
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_TRUNC, 0644)
	require.NoError(t, err)
	newContent := make([]byte, 800) // larger than previous 500
	for i := range newContent {
		newContent[i] = 'B' // different content → different fingerprint
	}
	_, err = f.Write(newContent)
	require.NoError(t, err)
	f.Close()

	// Verify inode is preserved
	info, err := os.Stat(logPath)
	require.NoError(t, err)
	assert.Equal(t, savedInode, getInode(info), "inode should be preserved after copy-truncate")

	action.uploadActiveLog(testCtx, gcDir)

	uploads := capture.get()
	require.Equal(t, 1, len(uploads))
	assert.Equal(t, newContent, uploads[0].body,
		"after copy-truncate + regrow, entire file should be uploaded")
	assert.Equal(t, int64(len(newContent)), action.activeBytesSent)
	assert.NotEmpty(t, action.activeTargetUrl)
}

func TestUploadActiveLog_NoNewData(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	gcDir := t.TempDir()
	writeFile(t, filepath.Join(gcDir, "gc.log"), []byte("static content"))

	action := newTestAction()
	// First call: uploads
	action.uploadActiveLog(testCtx, gcDir)
	require.Equal(t, 1, len(capture.get()))
	capture.reset()

	// Second call: no changes
	action.uploadActiveLog(testCtx, gcDir)
	assert.Empty(t, capture.get(), "no upload expected when no new data")
}

func TestUploadActiveLog_EmptyFile(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	gcDir := t.TempDir()
	writeFile(t, filepath.Join(gcDir, "gc.log"), []byte{})

	action := newTestAction()
	action.uploadActiveLog(testCtx, gcDir)

	assert.Empty(t, capture.get())
}

func TestUploadActiveLog_MissingFile(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	action := newTestAction()
	action.uploadActiveLog(testCtx, t.TempDir())

	assert.Empty(t, capture.get())
}

func TestUploadActiveLog_UrlContainsGcLogName(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	gcDir := t.TempDir()
	writeFile(t, filepath.Join(gcDir, "gc.log"), []byte("data"))

	action := newTestAction()
	action.uploadActiveLog(testCtx, gcDir)

	uploads := capture.get()
	require.Equal(t, 1, len(uploads))
	assert.True(t, strings.HasSuffix(uploads[0].path, "/gc.log"),
		"upload URL should end with /gc.log, got: %s", uploads[0].path)
	assert.False(t, strings.Contains(uploads[0].path, ".tmp"),
		"upload URL should not contain temp file name")
}

// --- Integration tests ---

func TestCollectGcLogs_FolderDoesNotExist(t *testing.T) {
	t.Setenv(constants.NcDiagLogFolder, t.TempDir())

	action := newTestAction()
	err := action.CollectGcLogs(testCtx)
	assert.NoError(t, err)
}

func TestCollectGcLogs_FullFlow(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	tmpDir := t.TempDir()
	gcDir := filepath.Join(tmpDir, constants.GcLogSubFolder)
	t.Setenv(constants.NcDiagLogFolder, tmpDir)

	writeFile(t, filepath.Join(gcDir, "gc.log.0"), []byte("rotated-0"))
	writeFile(t, filepath.Join(gcDir, "gc.log"), []byte("active-log-data"))

	action := newTestAction()
	err := action.CollectGcLogs(testCtx)
	assert.NoError(t, err)

	uploads := capture.get()
	assert.Equal(t, 2, len(uploads), "1 rotated + 1 active")

	_, err = os.Stat(filepath.Join(gcDir, "gc.log.0"))
	assert.True(t, os.IsNotExist(err), "rotated file should be deleted")

	_, err = os.Stat(filepath.Join(gcDir, "gc.log"))
	assert.NoError(t, err, "active file should still exist")

	// Second call with no changes → no uploads
	capture.reset()
	err = action.CollectGcLogs(testCtx)
	assert.NoError(t, err)
	assert.Empty(t, capture.get(), "no uploads when nothing changed")
}

func TestCollectGcLogs_FullRotationCycle(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	tmpDir := t.TempDir()
	gcDir := filepath.Join(tmpDir, constants.GcLogSubFolder)
	t.Setenv(constants.NcDiagLogFolder, tmpDir)

	logPath := filepath.Join(gcDir, "gc.log")

	// Step 1: initial active log
	writeFile(t, logPath, []byte("initial data"))
	action := newTestAction()
	action.CollectGcLogs(testCtx)
	assert.Equal(t, 1, len(capture.get()), "initial upload")
	capture.reset()

	// Step 2: JVM rotates: gc.log → gc.log.0, creates new gc.log
	os.Rename(logPath, filepath.Join(gcDir, "gc.log.0"))
	writeFile(t, logPath, []byte("after rotation"))

	action.CollectGcLogs(testCtx)
	uploads := capture.get()
	// Should have: 1 rotated (gc.log.0) + 1 active (new gc.log)
	assert.Equal(t, 2, len(uploads))

	// gc.log.0 should be deleted
	_, err := os.Stat(filepath.Join(gcDir, "gc.log.0"))
	assert.True(t, os.IsNotExist(err))

	// Old active URL may or may not be deleted depending on whether
	// the timestamp changed (same-second rotation → no DELETE)
}
