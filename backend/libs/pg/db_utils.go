package pg

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

func (c *Client) executeTransaction(ctx context.Context, query string, arguments ...any) error {
	startTime := time.Now()
	log.ExtraTrace(ctx, "start execution transaction for %s", query)

	tx, err := c.conn.Begin(ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, query, arguments...)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	log.ExtraTrace(ctx, "Transaction is finished in %v", time.Since(startTime))
	return nil
}

func jsonToMap[T any](inputJson string) (T, error) {
	var result T
	if err := json.Unmarshal([]byte(inputJson), &result); err != nil {
		return result, err
	}
	return result, nil
}
