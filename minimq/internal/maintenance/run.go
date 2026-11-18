// Package maintenance drives the broker's scheduled partition work.
package maintenance

import (
	"context"
	"time"
)

const CheckpointInterval = 30 * time.Second
const scheduleFollowUpDelay = 100 * time.Millisecond

// Run performs one extra scheduled-release batch after a regular round that may
// have reached its batch limit. Retries run on every regular round.
func Run(ctx context.Context, schedule func() bool, retry, checkpoint func()) {
	due := time.NewTicker(250 * time.Millisecond)
	compaction := time.NewTicker(CheckpointInterval)
	defer due.Stop()
	defer compaction.Stop()
	var followUp <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-due.C:
			followUp = nil
			mayHaveMore := schedule()
			retry()
			if mayHaveMore {
				followUp = time.After(scheduleFollowUpDelay)
			}
		case <-followUp:
			followUp = nil
			schedule()
		case <-compaction.C:
			checkpoint()
		}
	}
}
