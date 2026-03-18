package actions

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Netcracker/qubership-profiler-agent/diagtools/constants"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunScan_HprofRemovedAfterUpload(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	dumpDir := t.TempDir()

	// Create a fake .hprof file
	hprofPath := filepath.Join(dumpDir, "20260318T120000.hprof")
	writeFile(t, hprofPath, []byte("fake heap dump content"))

	action := ScanAction{
		Action: Action{
			DcdEnabled: true,
			DumpPath:   dumpDir,
			PodName:    "test-pod",
		},
	}

	pattern := filepath.Join(dumpDir, constants.DumpFilePattern)
	err := action.RunScan(testCtx, []string{pattern})
	require.NoError(t, err)

	// Original .hprof must be removed (ZipDump deletes it)
	_, err = os.Stat(hprofPath)
	assert.True(t, os.IsNotExist(err), ".hprof should be deleted after zip, but still exists")

	// .hprof.zip must also be removed after successful upload
	zipPath := hprofPath + ".zip"
	_, err = os.Stat(zipPath)
	assert.True(t, os.IsNotExist(err), ".hprof.zip should be deleted after upload, but still exists")

	// Verify the file was actually uploaded
	uploads := capture.get()
	require.Equal(t, 1, len(uploads), "expected exactly 1 upload")
}

func TestRunScan_SecondScanDoesNotReupload(t *testing.T) {
	capture := &uploadCapture{}
	srv := setupTestServer(capture)
	defer srv.Close()
	setupTestEnv(t, srv.URL)

	dumpDir := t.TempDir()

	hprofPath := filepath.Join(dumpDir, "20260318T120000.hprof")
	writeFile(t, hprofPath, []byte("fake heap dump content"))

	action := ScanAction{
		Action: Action{
			DcdEnabled: true,
			DumpPath:   dumpDir,
			PodName:    "test-pod",
		},
	}

	pattern := filepath.Join(dumpDir, constants.DumpFilePattern)

	// First scan: zip + upload + cleanup
	err := action.RunScan(testCtx, []string{pattern})
	require.NoError(t, err)
	require.Equal(t, 1, len(capture.get()), "first scan should upload once")
	capture.reset()

	// Second scan: nothing to process
	action2 := ScanAction{
		Action: Action{
			DcdEnabled: true,
			DumpPath:   dumpDir,
			PodName:    "test-pod",
		},
	}
	err = action2.RunScan(testCtx, []string{pattern})
	require.NoError(t, err)

	assert.Empty(t, capture.get(), "second scan should not upload anything")
}
