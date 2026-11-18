package main

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)

const (
	ttlByteBudget   = 4 << 20
	ttlByteReserved = ttlShardSlots * 112 // Store's fixed slot reservation per shard.
	ttlByteSamples  = 16
	ttlRefillSize   = 64 << 10
	ttlByteHotSlot  = 1
)

type ttlByteVisit struct {
	Read      outcome
	Fill      outcome
	Backend   bool
	ElapsedUS int64
}

type ttlByteWork struct {
	Overwrite   outcome
	ExpiredRead outcome
	Visits      [3]ttlByteVisit
	ProtectedNX outcome
	Preserved   bool
	Refilled    bool
}

type ttlByteWave struct {
	BeforeUS int64
	Before   []ttlSample
	AfterUS  int64
	After    []ttlSample
}

func runTTLBytes(t target, pattern string) map[string]any {
	slots := 512
	if pattern == "byte-limit" {
		slots = 1024
	}
	result := make(map[string]any)
	fail := func(message string) map[string]any {
		result["SetupError"] = message
		return result
	}
	layout, err := ttlLayout(slots, 1)
	if err != nil {
		return fail(err.Error())
	}
	keyBytes := len(layout[0][0].key)
	charge := (ttlByteBudget - ttlByteReserved - smallSize - keyBytes) / (slots - 1)
	oldValue := bytes.Repeat([]byte("o"), charge-keyBytes)
	hotValue := bytes.Repeat([]byte("h"), smallSize)
	first := time.Now().Add(4 * time.Second)
	fill := populateTTL(t, layout, slots, func(slot int) ([]byte, time.Time) {
		if slot == ttlByteHotSlot {
			return hotValue, time.Time{}
		}
		if slot%4 != ttlByteHotSlot {
			return oldValue, first
		}
		return oldValue, time.Time{}
	})
	result["Prefill"] = fill
	if fill[ok] != ttlShards*slots || time.Until(first) < 650*time.Millisecond {
		return fail("did not finish prefill before the read window")
	}
	prefill, err := ttlShardStats(t)
	if err != nil {
		return fail(err.Error())
	}
	for _, shard := range prefill {
		if shard.Entries != slots || shard.UsedBytes != int64(ttlByteReserved+charge*(slots-1)+smallSize+keyBytes) || shard.Evicted != 0 || shard.Expired != 0 {
			return fail(fmt.Sprintf("unexpected prefill accounting: %+v", shard))
		}
	}
	result["PrefillShards"] = prefill
	result["ShardBudgetBytes"] = ttlByteBudget
	result["PrefillObjectCharge"] = charge
	result["RefillObjectCharge"] = ttlRefillSize + keyBytes
	result["MaxCandidateBytes"] = ttlByteSamples * charge
	result["ExpiringPerShard"] = slots*3/4 - 1 // Slot zero is overwritten below.
	last := first
	for _, keys := range layout {
		for _, key := range keys[1:slots] {
			if key.upper.After(last) {
				last = key.upper
			}
		}
	}
	result["LastDeadlineUS"] = last.Sub(first).Microseconds()
	finishReads := startTTLReads(t, layout, ttlByteHotSlot, first)
	var wg sync.WaitGroup
	var clients [ttlShards]client
	var work [ttlShards]ttlByteWork
	defer func() {
		for i := range clients {
			clients[i].close()
		}
	}()
	overwrite := bytes.Repeat([]byte("r"), len(oldValue))
	time.Sleep(time.Until(first.Add(-150 * time.Millisecond)))
	for i := range clients {
		clients[i].target = t
		wg.Go(func() {
			_, work[i].Overwrite = clients[i].execute(ttlWrite(layout[i][0].key, overwrite, "NONE", false), 0, 0)
		})
	}
	wg.Wait()
	if !time.Now().Before(first) {
		finishReads()
		return fail("overwrites did not complete before the target deadline")
	}
	for _, w := range work {
		if w.Overwrite != ok {
			finishReads()
			return fail("could not replace the expiring key before its deadline")
		}
	}
	var waves [3]ttlByteWave
	for visit, offset := range [...]time.Duration{time.Millisecond, 30 * time.Millisecond, 120 * time.Millisecond} {
		time.Sleep(time.Until(last.Add(offset)))
		wave := &waves[visit]
		wave.BeforeUS = time.Since(first).Microseconds()
		wave.Before, err = ttlShardStats(t)
		if err != nil {
			finishReads()
			return fail(err.Error())
		}
		for i := range clients {
			wg.Go(func() {
				work[i].Visits[visit] = ttlByteRefill(&clients[i], layout[i][slots].key)
			})
		}
		wg.Wait()
		wave.After, err = ttlShardStats(t)
		wave.AfterUS = time.Since(first).Microseconds()
		if err != nil {
			finishReads()
			return fail(err.Error())
		}
	}
	result["Waves"] = waves
	result["ReadSchedule"], result["Reads"] = finishReads()
	for i := range clients {
		wg.Go(func() {
			c, keys := &clients[i], layout[i]
			_, work[i].ExpiredRead = c.execute(t.makeCommand(keys[2].key, nil, len(oldValue)), len(oldValue), 0)
			_, work[i].ProtectedNX = c.execute(ttlWrite(keys[0].key, oldValue, "NONE", true), 0, 0)
			work[i].Preserved = ttlValueMatches(c, keys[0].key, overwrite)
			work[i].Refilled = ttlValueMatches(c, keys[slots].key, ttlBackendValue())
		})
	}
	wg.Wait()
	result["Work"] = work
	end, err := ttlShardStats(t)
	if err != nil {
		return fail(err.Error())
	}
	result["End"] = end
	return result
}

func ttlByteRefill(c *client, key string) ttlByteVisit {
	started := time.Now()
	_, status := c.execute(c.target.makeCommand(key, nil, ttlRefillSize), ttlRefillSize, 0)
	visit := ttlByteVisit{Read: status}
	if status == miss {
		visit.Backend = true
		value := ttlBackendValue()
		_, visit.Fill = c.execute(ttlWrite(key, value, "NONE", true), 0, 0)
	}
	visit.ElapsedUS = time.Since(started).Microseconds()
	return visit
}

func ttlBackendValue() []byte {
	return bytes.Repeat([]byte("b"), ttlRefillSize)
}
