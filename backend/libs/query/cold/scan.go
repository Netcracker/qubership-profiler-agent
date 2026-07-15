package cold

import (
	"context"
	"io"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

// traceBlobField is the CallV2 struct field backing the trace_blob column;
// dropColumn matches it by the reader's in-name path.
const traceBlobField = "TraceBlob"

// ScanFile reads one discovered parquet file with column projection — the
// trace_blob column chunks are never read on the list path (02 §5.4,
// §2.3.2) — applies the row filter ts_ms ∈ [from, to) plus the §2.3
// predicates and the keyset seek, and returns the surviving rows in the
// file's native (ts_ms DESC, pk ASC) order (01 §5.2), ready to be one merge
// run. A listed-but-deleted object returns an empty result (§5.1).
func ScanFile(ctx context.Context, store ObjectStore, ref FileRef, q model.CallsQuery, after *model.Position) ([]model.CallRow, error) {
	obj, err := store.Open(ctx, ref.Key)
	if errors.Is(err, ErrNotFound) {
		return nil, nil // compacted away after the LIST (02 §5.1)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "open %s", ref.Key)
	}
	defer func() { _ = obj.Close() }()

	pr, err := reader.NewParquetReader(&objectFile{obj: obj, size: obj.Size()}, new(storageparquet.CallV2), 1)
	if err != nil {
		return nil, errors.Wrapf(err, "read parquet footer of %s", ref.Key)
	}
	defer pr.ReadStop()
	if err := dropColumn(pr, traceBlobField); err != nil {
		return nil, errors.Wrapf(err, "project %s", ref.Key)
	}

	rows := make([]storageparquet.CallV2, pr.GetNumRows())
	if err := pr.Read(&rows); err != nil {
		return nil, errors.Wrapf(err, "read %s", ref.Key)
	}

	out := make([]model.CallRow, 0, len(rows))
	for i := range rows {
		row := toCallRow(&rows[i])
		if !q.Match(row) {
			continue
		}
		if after != nil && !row.Position().After(*after) {
			continue
		}
		out = append(out, row)
	}
	return out, nil
}

// dropColumn removes one column's buffer before any row is read, so its
// chunks are never fetched: NewParquetReader only positions a reader at the
// chunk offset, the data pages load lazily on Read. This is the projection
// seam — the schema stays the full CallV2 and the unmarshaller leaves the
// dropped field nil.
func dropColumn(pr *reader.ParquetReader, field string) error {
	suffix := common.PAR_GO_PATH_DELIMITER + field
	for path, cb := range pr.ColumnBuffers {
		if strings.HasSuffix(path, suffix) {
			delete(pr.ColumnBuffers, path)
			return cb.PFile.Close()
		}
	}
	return errors.Errorf("column %s not found in schema", field)
}

// toCallRow maps a projected CallV2 row to the merged row shape.
func toCallRow(v *storageparquet.CallV2) model.CallRow {
	row := model.CallRow{
		PK: model.PK{
			PodNamespace:   v.Namespace,
			PodService:     v.ServiceName,
			PodName:        v.PodName,
			RestartTimeMs:  v.RestartTimeMs,
			TraceFileIndex: v.TraceFileIndex,
			BufferOffset:   v.BufferOffset,
			RecordIndex:    v.RecordIndex,
		},
		TsMs:           v.TsMs,
		DurationMs:     v.DurationMs,
		Method:         v.Method,
		ThreadName:     v.ThreadName,
		CpuTimeMs:      v.CpuTimeMs,
		WaitTimeMs:     v.WaitTimeMs,
		MemoryUsed:     v.MemoryUsed,
		ChildCalls:     v.ChildCalls,
		ErrorFlag:      v.ErrorFlag,
		RetentionClass: v.RetentionClass,
		Tier:           model.TierCold,
	}
	if v.TruncatedReason != nil {
		row.TruncatedReason = *v.TruncatedReason
	}
	if len(v.Params) > 0 {
		row.Params = make(map[string][]string, len(v.Params))
		for k, vals := range v.Params {
			if vals != nil {
				row.Params[k] = vals.ValueList
			}
		}
	}
	return row
}

// objectFile adapts an Object to the parquet-go source.ParquetFile surface:
// a stateless ReaderAt shared by per-column handles, each with its own
// cursor. Close is a no-op — the reader closes per-column handles, while the
// underlying object is owned and closed by ScanFile once.
type objectFile struct {
	obj  Object
	size int64
	pos  int64
}

var _ source.ParquetFile = (*objectFile)(nil)

func (f *objectFile) Read(p []byte) (int, error) {
	if f.pos >= f.size {
		return 0, io.EOF
	}
	n, err := f.obj.ReadAt(p, f.pos)
	f.pos += int64(n)
	if errors.Is(err, io.EOF) && n > 0 {
		err = nil // partial tail read; the next call reports EOF
	}
	return n, err
}

func (f *objectFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = f.size + offset
	default:
		return 0, errors.Errorf("unsupported whence %d", whence)
	}
	if f.pos < 0 {
		return 0, errors.New("negative seek position")
	}
	return f.pos, nil
}

func (f *objectFile) Open(string) (source.ParquetFile, error) {
	return &objectFile{obj: f.obj, size: f.size}, nil
}

func (f *objectFile) Close() error { return nil }

func (f *objectFile) Write([]byte) (int, error) {
	return 0, errors.New("cold parquet objects are read-only")
}

func (f *objectFile) Create(string) (source.ParquetFile, error) {
	return nil, errors.New("cold parquet objects are read-only")
}
