// Package maintenance drives the broker's scheduled partition work.
package maintenance

import (
	"context"
	"time"
)

const CheckpointInterval = 30 * time.Second

func Run(ctx context.Context, advance, checkpoint func()) {
	due := time.NewTicker(250 * time.Millisecond)
	compaction := time.NewTicker(CheckpointInterval)
	defer due.Stop()
	defer compaction.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-due.C:
			advance()
		case <-compaction.C:
			checkpoint()
		}
	}
}
