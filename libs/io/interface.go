package io

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/common"
)

type (
	Reader interface {
		EOF() bool
		Done()
		Pos() uint64
		ReadFixedByte(ctx context.Context) (byte, error)
		ReadFixedInt(ctx context.Context) (int, error)
		ReadFixedLong(ctx context.Context) (uint64, error)
		ReadUuid(ctx context.Context) (common.Uuid, error)
		ReadFixedString(ctx context.Context) (string, error)
	}

	Writer interface {
		WriteFixedByte(ctx context.Context, v byte) error
		WriteFixedInt(ctx context.Context, v int) error
		WriteFixedLong(ctx context.Context, v uint64) error
		WriteUuid(ctx context.Context, v common.Uuid) error
		WriteFixedString(ctx context.Context, v string) error
		WriteFixedBuf(ctx context.Context, v []byte) error
		Flush() error
	}

	Debugger interface {
		PrintDebug(ctx context.Context)
	}
)
