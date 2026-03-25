package view

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTraceTreeRowId_Compare(t *testing.T) {
	t.Run("compare", func(t *testing.T) {
		t1 := rowId(1, 2, 3)
		assert.Equal(t, 0, t1.Compare(rowId(1, 2, 3)))

		assert.Equal(t, 1, t1.Compare(rowId(1, 2, 0)))
		assert.Equal(t, -1, t1.Compare(rowId(1, 2, 45)))

		assert.Equal(t, 1, t1.Compare(rowId(1, 0, 0)))
		assert.Equal(t, -1, t1.Compare(rowId(1, 43, 0)))
		assert.Equal(t, -1, t1.Compare(rowId(1, 43, 3)))
		assert.Equal(t, -1, t1.Compare(rowId(1, 43, 2343)))

		assert.Equal(t, 1, t1.Compare(rowId(0, 2, 0)))
		assert.Equal(t, -1, t1.Compare(rowId(4, 2, 3)))
	})
}

func TestTraceTreeRowId_String(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		row := rowId(1, 0, 0)
		assert.Equal(t, "TreeRowid{traceFileIndex=1, bufferOffset=0, recordIndex=0, reactorFileIndex=0, reactorBufferOffset=0, fullRowId=101_1_0_0, folderId=101}", row.String())

		row = rowId(1, 10, 12)
		assert.Equal(t, "TreeRowid{traceFileIndex=1, bufferOffset=10, recordIndex=12, reactorFileIndex=0, reactorBufferOffset=0, fullRowId=101_1_10_12, folderId=101}", row.String())

		row = rowId(1, 0, 12)
		assert.Equal(t, "TreeRowid{traceFileIndex=1, bufferOffset=0, recordIndex=12, reactorFileIndex=0, reactorBufferOffset=0, fullRowId=101_1_0_12, folderId=101}", row.String())

		row = rowId(1, 34_232_434, 0)
		assert.Equal(t, "TreeRowid{traceFileIndex=1, bufferOffset=34232434, recordIndex=0, reactorFileIndex=0, reactorBufferOffset=0, fullRowId=101_1_34232434_0, folderId=101}", row.String())

		row = rowId(2, 0, 0)
		assert.Equal(t, "TreeRowid{traceFileIndex=2, bufferOffset=0, recordIndex=0, reactorFileIndex=0, reactorBufferOffset=0, fullRowId=101_2_0_0, folderId=101}", row.String())
	})
}

func rowId(file, offset, record int) TraceTreeRowId {
	return CreateTreeRowId(101, file, offset, record)
}
