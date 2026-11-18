package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"
)

const (
	ttlShards     = 16
	ttlShardSlots = 4096
	ttlReads      = 16
)

type ttlEntry struct {
	key   string
	upper time.Time
}

type ttlSample struct {
	OffsetMS       int64
	Expired        uint64
	Evicted        uint64
	Rejected       uint64
	Entries        int
	UsedBytes      int64
	Due            int
	PendingAtLeast int
}

type ttlWork struct {
	Overlap               outcome
	Preserved             bool
	Probes                map[outcome]int
	NXBefore              outcome
	NXRace                [2]outcome
	NXValue               bool
	NXAfterSweep          bool
	Refills               map[outcome]int
	RefillP99Microseconds float64
}

func runTTL(t target, pattern string) map[string]any {
	result := make(map[string]any)
	layout, err := ttlLayout(ttlShardSlots, 4)
	if err != nil {
		result["SetupError"] = err.Error()
		return result
	}
	oldValue := bytes.Repeat([]byte("o"), smallSize)
	firstDeadline := time.Now().Add(4 * time.Second)
	fill := populateTTL(t, layout, ttlShardSlots, func(slot int) ([]byte, time.Time) {
		if slot%2 == 0 {
			return oldValue, ttlDeadline(firstDeadline, pattern, slot)
		}
		return oldValue, time.Time{}
	})
	result["Prefill"] = fill
	if fill[ok] != ttlShards*ttlShardSlots || time.Until(firstDeadline) < 650*time.Millisecond {
		result["SetupError"] = "could not prefill all slots before the read window"
		return result
	}
	before, err := ttlStats(t, time.Now(), firstDeadline, nil)
	if err != nil || before.Entries != ttlShards*ttlShardSlots || before.Evicted != 0 || before.Expired != 0 {
		result["SetupError"] = fmt.Sprintf("prefill not full or no longer stable: %+v (%v)", before, err)
		return result
	}
	result["Before"] = before

	// Track expiration brackets from the TCP reply. A completed SET is
	// necessarily published by the time its response reaches the driver.
	var deadlines []time.Time
	for _, keys := range layout {
		for i := 2; i < ttlShardSlots; i += 2 {
			deadlines = append(deadlines, keys[i].upper)
		}
	}
	slices.SortFunc(deadlines, func(a, b time.Time) int { return a.Compare(b) })
	lastDeadline := deadlines[len(deadlines)-1]
	result["LastDeadlineMS"] = lastDeadline.Sub(firstDeadline).Milliseconds()

	finishReads := startTTLReads(t, layout, 3073, firstDeadline)

	refillStart := firstDeadline.Add(40 * time.Millisecond)
	work := make([]ttlWork, ttlShards)
	var wg sync.WaitGroup
	for shard := range ttlShards {
		wg.Go(func() { work[shard] = exerciseTTL(t, layout[shard], firstDeadline, refillStart) })
	}
	at := firstDeadline.Add(15 * time.Millisecond)
	time.Sleep(time.Until(at))
	refillBefore, err := ttlStats(t, time.Now(), firstDeadline, deadlines)
	if err == nil {
		result["BeforeRefill"] = refillBefore
	}

	samples := make([]ttlSample, 0, 35)
	for offset := 100 * time.Millisecond; offset <= 6500*time.Millisecond; offset += 200 * time.Millisecond {
		time.Sleep(time.Until(firstDeadline.Add(offset)))
		sample, err := ttlStats(t, time.Now(), firstDeadline, deadlines)
		if err == nil {
			samples = append(samples, sample)
		}
	}
	wg.Wait()
	result["Samples"] = samples
	result["ShardWork"] = work
	result["ReadSchedule"], result["Reads"] = finishReads()
	return result
}

func startTTLReads(t target, layout [][]ttlEntry, hotSlot int, first time.Time) func() (map[string]int, map[string]*pressurePhase) {
	var wg sync.WaitGroup
	reads := make([][]pressureRead, ttlReads)
	jobs := make(chan arrival, 256)
	var scheduled, unused counts
	readStart := first.Add(-400 * time.Millisecond)
	go schedule(readStart, 5*time.Second, 250*time.Microsecond, 10000, jobs, make(chan arrival, 8), &scheduled, &unused)
	for shard := range ttlReads {
		wg.Go(func() {
			c := client{target: t}
			defer c.close()
			get := t.makeCommand(layout[shard][hotSlot].key, nil, smallSize)
			for job := range jobs {
				started := time.Now()
				_, status := c.execute(get, smallSize, 0)
				end := time.Now()
				reads[shard] = append(reads[shard], pressureRead{job.at, end.Sub(job.at), end.Sub(started), status})
			}
		})
	}
	return func() (map[string]int, map[string]*pressurePhase) {
		wg.Wait()
		return map[string]int{"Offered": scheduled.Offered, "Waiting": scheduled.Waiting, "Late": scheduled.Late},
			pressureReadPhases(reads, first.Add(-50*time.Millisecond), first.Add(500*time.Millisecond))
	}
}

func ttlLayout(slots, refillKeys int) ([][]ttlEntry, error) {
	layout := make([][]ttlEntry, ttlShards)
	remaining := ttlShards * (slots + refillKeys)
	keys := make([]string, 1024)
	for base := 0; remaining > 0; base += len(keys) {
		for i := range keys {
			keys[i] = fmt.Sprintf("ttl%06d", base+i)
		}
		routes, err := pressureKeyRoutes(keys)
		if err != nil {
			return nil, err
		}
		for i, shard := range routes {
			if shard < 0 || shard >= ttlShards {
				return nil, fmt.Errorf("invalid shard %d", shard)
			}
			if len(layout[shard]) < slots+refillKeys {
				layout[shard] = append(layout[shard], ttlEntry{key: keys[i]})
				remaining--
			}
		}
	}
	return layout, nil
}

func ttlDeadline(first time.Time, pattern string, slot int) time.Time {
	if pattern == "spread" {
		return first.Add(time.Duration((slot/2)%16) * 200 * time.Millisecond)
	}
	return first
}

func ttlWrite(key string, value []byte, ttl string, nx bool) command {
	option := ""
	if nx {
		option = " NX"
	}
	header := fmt.Appendf(make([]byte, 0, len(value)+128), "SET %d %d %s%s\n%s", len(key), len(value), ttl, option, key)
	return command{frame: append(header, value...), reply: "STORED\n", set: true}
}

func populateTTL(t target, layout [][]ttlEntry, slots int, object func(int) ([]byte, time.Time)) map[outcome]int {
	var wg sync.WaitGroup
	partial := make([]map[outcome]int, len(layout))
	for i, keys := range layout {
		wg.Go(func() {
			c := client{target: t}
			defer c.close()
			partial[i] = make(map[outcome]int)
			for slot := range slots {
				entry := &keys[slot]
				value, deadline := object(slot)
				ttl := "NONE"
				if !deadline.IsZero() {
					remaining := time.Until(deadline)
					if remaining <= 0 {
						partial[i][timeout]++
						continue
					}
					ms := (remaining + time.Millisecond - 1) / time.Millisecond
					ttl = fmt.Sprint(int64(ms))
					_, status := c.execute(ttlWrite(entry.key, value, ttl, false), 0, 0)
					partial[i][status]++
					entry.upper = time.Now().Add(ms * time.Millisecond)
					continue
				}
				_, status := c.execute(ttlWrite(entry.key, value, ttl, false), 0, 0)
				partial[i][status]++
			}
		})
	}
	wg.Wait()
	total := make(map[outcome]int)
	for _, counts := range partial {
		for status, n := range counts {
			total[status] += n
		}
	}
	return total
}

func exerciseTTL(t target, keys []ttlEntry, first, refillStart time.Time) ttlWork {
	r := ttlWork{Probes: make(map[outcome]int), Refills: make(map[outcome]int)}
	c, other := client{target: t}, client{target: t}
	defer c.close()
	defer other.close()
	replacement := bytes.Repeat([]byte("r"), smallSize)
	time.Sleep(time.Until(first.Add(-150 * time.Millisecond)))
	_, r.Overlap = c.execute(ttlWrite(keys[0].key, replacement, "NONE", false), 0, 0)

	var refillTimes []time.Duration
	time.Sleep(time.Until(refillStart))
	for _, key := range keys[ttlShardSlots:] {
		started := time.Now()
		_, status := c.execute(ttlWrite(key.key, replacement, "NONE", true), 0, 0)
		r.Refills[status]++
		refillTimes = append(refillTimes, time.Since(started))
	}
	slices.Sort(refillTimes)
	r.RefillP99Microseconds = percentile(refillTimes, 990)

	raceKey := keys[2]
	time.Sleep(time.Until(raceKey.upper.Add(20 * time.Millisecond)))
	_, r.NXBefore = c.execute(t.makeCommand(raceKey.key, nil, smallSize), smallSize, 0)
	alternative := bytes.Repeat([]byte("n"), smallSize)
	firstRace := make(chan outcome, 1)
	go func() {
		_, status := other.execute(ttlWrite(raceKey.key, alternative, "NONE", true), 0, 0)
		firstRace <- status
	}()
	_, r.NXRace[0] = c.execute(ttlWrite(raceKey.key, replacement, "NONE", true), 0, 0)
	r.NXRace[1] = <-firstRace
	winner := replacement
	if r.NXRace[1] == ok {
		winner = alternative
	}
	r.NXValue = ttlValueMatches(&c, raceKey.key, winner)

	for i := 4; i < 20; i += 2 {
		key := keys[i]
		time.Sleep(time.Until(key.upper.Add(20 * time.Millisecond)))
		_, status := c.execute(t.makeCommand(key.key, nil, smallSize), smallSize, 0)
		r.Probes[status]++
	}

	time.Sleep(time.Until(first.Add(6200 * time.Millisecond)))
	r.Preserved = ttlValueMatches(&c, keys[0].key, replacement)
	r.NXAfterSweep = ttlValueMatches(&c, raceKey.key, winner)
	return r
}

func ttlValueMatches(c *client, key string, expected []byte) bool {
	if c.conn == nil {
		return false
	}
	get := c.target.makeCommand(key, nil, len(expected))
	if c.conn.SetDeadline(time.Now().Add(time.Second)) != nil {
		return false
	}
	if _, err := c.conn.Write(get.frame); err != nil {
		return false
	}
	line, err := c.reader.ReadString('\n')
	if err != nil || line != get.reply {
		return false
	}
	value := make([]byte, len(expected))
	_, err = io.ReadFull(c.reader, value)
	return err == nil && bytes.Equal(value, expected)
}

func ttlStats(t target, at, first time.Time, deadlines []time.Time) (ttlSample, error) {
	shards, err := ttlShardStats(t)
	if err != nil {
		return ttlSample{}, err
	}
	total := ttlSample{OffsetMS: at.Sub(first).Milliseconds()}
	for _, shard := range shards {
		total.Expired += shard.Expired
		total.Evicted += shard.Evicted
		total.Rejected += shard.Rejected
		total.Entries += shard.Entries
		total.UsedBytes += shard.UsedBytes
	}
	due, _ := slices.BinarySearchFunc(deadlines, at, func(deadline, now time.Time) int {
		if !deadline.After(now) {
			return -1
		}
		return 1
	})
	total.Due = due
	// All live evictions may have been expiring keys, and each shard can
	// replace one expired key in place via NX. Subtract both conservatively.
	total.PendingAtLeast = max(0, due-int(total.Expired+total.Evicted)-ttlShards)
	return total, nil
}

func ttlShardStats(t target) ([]ttlSample, error) {
	raw, err := diagnostics(t)
	if err != nil {
		return nil, err
	}
	var stats struct {
		Shards []ttlSample
	}
	if err := json.Unmarshal(raw, &stats); err != nil || len(stats.Shards) != ttlShards {
		return nil, fmt.Errorf("could not decode all shard stats")
	}
	return stats.Shards, nil
}
