package pg

import (
	"context"
	"fmt"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"

	"github.com/Netcracker/qubership-profiler-backend/libs/pg/queries"
)

func (c *Client) UpdateTempTableInventory(ctx context.Context, info inventory.TempTableInfo) error {
	query := fmt.Sprintf(queries.UpdateTempTableInventory, TempTableInventoryTable)
	return c.executeTransaction(ctx, query, info.Status, info.RowsCount, info.TableSize, info.TableTotalSize, info.Uuid.Val)
}

func (c *Client) UpdateS3File(ctx context.Context, file inventory.S3FileInfo) error {
	query := fmt.Sprintf(queries.UpdateS3Files, S3FilesTable)
	return c.executeTransaction(ctx, query,
		file.Status, file.Services.List(), file.RowsCount, file.FileSize, file.RemoteStoragePath, file.Uuid.Val,
	)
}
