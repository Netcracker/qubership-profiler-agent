package parquet

import (
	"time"

	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"

	parquet2 "github.com/xitongsys/parquet-go/parquet"
)

const (
	DefaultRowGroupSize = 128 * 1024 * 1024 // 128Mb
	DefaultPageSize     = 4 * 1024 * 1024   // 4Mb
	DefaultCompression  = CompressionGzip

	CompressionNone = parquet2.CompressionCodec_UNCOMPRESSED
	CompressionGzip = parquet2.CompressionCodec_GZIP

	// CurrentVersion schema version for Parquet file
	CurrentVersion = model.ApiVersion
)

type (
	CompressionType = parquet2.CompressionCodec

	Params struct {
		RowGroupSize, PageSize int64 // parameters for library
		CompressionType        CompressionType
		BatchSize              int // local batching
		S3FileLifeTime         time.Duration
	}
)

var (
	DefaultParams = Params{
		RowGroupSize:    DefaultRowGroupSize,
		PageSize:        DefaultPageSize,
		CompressionType: DefaultCompression,
		BatchSize:       100,
		S3FileLifeTime:  time.Hour, // 1h
	}
)
