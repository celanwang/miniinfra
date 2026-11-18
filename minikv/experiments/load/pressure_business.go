package main

import (
	"bytes"
	"fmt"
	"io"
	"time"
)

const businessWriters = 8

var businessStoredSizes = [businessWriters]int{
	smallSize, 1024, 64 << 10, 128 << 10, (256 << 10) - 1, 256 << 10, largeSize / 2, largeSize - 1,
}

var businessRejectSizes = [...]int{
	64 << 10, (128 << 10) - 1, 128 << 10, (256 << 10) - 1, 256 << 10,
	384 << 10, largeSize / 2, 768 << 10, largeSize - 1, largeSize,
}

type businessPressure struct {
	stored, rejected []string
	prefill          map[outcome]int
}

func prepareBusinessPressure(target target) (*businessPressure, error) {
	const count = 100000
	keys := make([]string, 513)
	keys[0] = "business-root"
	for i := 1; i < len(keys); i++ {
		keys[i] = fmt.Sprintf("business%06d", i)
	}
	routes, err := pressureKeyRoutes(keys)
	if err != nil {
		return nil, err
	}
	writeShard := routes[0]
	rejectShard := (writeShard + 1) % 16
	business := &businessPressure{}
	for i, shard := range routes[1:] {
		if shard == writeShard && len(business.stored) < businessWriters {
			business.stored = append(business.stored, keys[i+1])
		}
		if shard == rejectShard && len(business.rejected) < businessWriters {
			business.rejected = append(business.rejected, keys[i+1])
		}
	}
	if len(business.stored) != businessWriters || len(business.rejected) != businessWriters {
		return nil, fmt.Errorf("could not route business keys")
	}

	// Leave room for overwrites on one shard while filling the other fifteen
	// with the same small-value objects as the capacity replay.
	prefill := make([]string, 0, count)
	batch := make([]string, 1024)
	for base := 0; len(prefill) < count; base += len(batch) {
		for i := range batch {
			batch[i] = fmt.Sprintf("fill%06d", base+i)
		}
		routes, err := pressureKeyRoutes(batch)
		if err != nil {
			return nil, err
		}
		for i, shard := range routes {
			if shard != writeShard {
				prefill = append(prefill, batch[i])
				if len(prefill) == count {
					break
				}
			}
		}
	}
	business.prefill = fillKeys(target, 32, count, 1024, func(i int) string { return prefill[i] })
	if business.prefill[ok] != count {
		return nil, fmt.Errorf("business prefill: %v", business.prefill)
	}
	return business, nil
}

func (b *businessPressure) seedSuccessKeys(c *client) error {
	for i, key := range b.stored {
		value := bytes.Repeat([]byte("p"), businessStoredSizes[i])
		if _, got := c.execute(c.target.makeCommand(key, value, len(value)), 0, 0); got != ok {
			return fmt.Errorf("seed overwrite %d: %s", i, got)
		}
	}
	return nil
}

func (b *businessPressure) write(index int) (string, int, outcome) {
	lane, step := index%businessWriters, index/businessWriters
	if (lane+step)%4 == 0 {
		size := businessStoredSizes[lane] + (step/4)%2
		return b.stored[lane], size, ok
	}
	// More 512 KiB–1 MiB frames in the latter half, interleaved with
	// successful overwrites and the smaller size classes.
	slot, choices := 0, 7
	if step >= 32 {
		slot, choices = 3, 7
	}
	size := businessRejectSizes[slot+(step*3+lane*2)%choices]
	return b.rejected[lane], size, noSpace
}

func (b *businessPressure) verify(c *client, lane int, expected []byte) bool {
	cmd := c.target.makeCommand(b.stored[lane], nil, len(expected))
	if c.conn.SetDeadline(time.Now().Add(time.Second)) != nil {
		return false
	}
	if _, err := c.conn.Write(cmd.frame); err != nil {
		return false
	}
	header, err := c.reader.ReadString('\n')
	if err != nil || header != cmd.reply {
		return false
	}
	value := make([]byte, len(expected))
	_, err = io.ReadFull(c.reader, value)
	return err == nil && bytes.Equal(value, expected)
}
