package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg/queries"
)

func (c *Client) RemoveTempTableInventory(ctx context.Context, uuid common.Uuid) error {
	startTime := time.Now()
	log.Debug(ctx, "[RemoveTempTableInventory] uuid = %s ", uuid.String())

	query := fmt.Sprintf(queries.RemoveByUUID, TempTableInventoryTable)
	err := c.executeTransaction(ctx, query, uuid.Val)

	log.Debug(ctx, "RemoveTempTableInventory is finished. [Execution time - %v]", time.Since(startTime))
	return err
}

func (c *Client) RemoveS3File(ctx context.Context, uuid common.Uuid) error {
	startTime := time.Now()
	log.Debug(ctx, "[RemoveS3File] uuid = %s ", uuid.String())

	query := fmt.Sprintf(queries.RemoveByUUID, S3FilesTable)
	err := c.executeTransaction(ctx, query, uuid.Val)

	log.Debug(ctx, "RemoveS3File is finished. [Execution time - %v]", time.Since(startTime))
	return err
}

func (c *Client) RemovePod(ctx context.Context, podId string) error {
	startTime := time.Now()
	log.Debug(ctx, "[RemovePod] podId = %s ", podId)

	query := fmt.Sprintf(queries.RemoveByPodId, PodsTable)
	err := c.executeTransaction(ctx, query, podId)

	log.Debug(ctx, "RemovePod is finished. [Execution time - %v]", time.Since(startTime))
	return err
}

func (c *Client) RemovePodRestart(ctx context.Context, podId string) error {
	startTime := time.Now()
	log.Debug(ctx, "[RemovePodRestart] podId = %s ", podId)

	query := fmt.Sprintf(queries.RemoveByPodId, PodRestartsTable)
	err := c.executeTransaction(ctx, query, podId)

	log.Debug(ctx, "RemovePodRestart is finished. [Execution time - %v]", time.Since(startTime))
	return err
}
