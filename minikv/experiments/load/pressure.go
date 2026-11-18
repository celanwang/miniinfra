package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"
)

type pressureRead struct {
	at      time.Time
	elapsed time.Duration
	result  outcome
}

type pressurePhase struct {
	Reads           map[outcome]int
	P99Microseconds float64
}

func runPressure(target target, lengths string) map[string]any {
	result := make(map[string]any)
	if lengths != "publish" {
		prefill := fill(target, "fill", 32, 100000, 1024)
		result["Prefill"] = prefill
		if prefill[ok] != 100000 {
			return result
		}
	}

	const readers = 16
	const writers = 8
	hot := make([]command, readers)
	warm := client{target: target}
	defer warm.close()
	small := bytes.Repeat([]byte("r"), smallSize)
	for i := range hot {
		key := fmt.Sprintf("read%06d", i)
		if _, status := warm.execute(target.makeCommand(key, small, smallSize), 0, 0); status != ok {
			result["WarmReadKeyError"] = status
			return result
		}
		hot[i] = target.makeCommand(key, nil, smallSize)
	}
	if lengths == "publish" {
		payload := bytes.Repeat([]byte("p"), largeSize-1)
		for i := range 2 {
			cmd := target.makeCommand(pressureWriteKey(lengths, i), payload, len(payload))
			if _, status := warm.execute(cmd, 0, 0); status != ok {
				result["WarmWriteKeyError"] = status
				return result
			}
		}
	}
	warm.close()
	before, _ := diagnostics(target)
	result["Before"] = before

	var workers sync.WaitGroup
	reads := make([][]pressureRead, readers)
	queue := make(chan arrival, 256)
	for i := range readers {
		workers.Go(func() {
			client := client{target: target}
			defer client.close()
			for job := range queue {
				_, status := client.execute(hot[i], smallSize, 0)
				reads[i] = append(reads[i], pressureRead{job.at, time.Since(job.at), status})
			}
		})
	}
	start := time.Now().Add(100 * time.Millisecond)
	var scheduled, auxiliary counts
	go schedule(start, 2*time.Second, 250*time.Microsecond, 10000, queue, make(chan arrival, 8), &scheduled, &auxiliary)

	httpClient := http.Client{Timeout: 40 * time.Millisecond}
	first := readRuntime(&httpClient)
	peak := make(map[string]float64)
	for k, v := range first {
		peak[k] = v
	}
	stopSampling := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				for key, value := range readRuntime(&httpClient) {
					peak[key] = max(peak[key], value)
				}
			}
		}
	}()

	time.Sleep(time.Until(start.Add(250 * time.Millisecond)))
	writeBefore := readRuntime(&httpClient)
	writeStart := time.Now()
	writeResults := make([]map[outcome]int, writers)
	writeLatency := make([][]time.Duration, writers)
	recovered := make([]bool, writers)
	value := bytes.Repeat([]byte("v"), largeSize)
	var writes sync.WaitGroup
	for i := range writers {
		writes.Go(func() {
			client := client{target: target}
			defer client.close()
			statuses := make(map[outcome]int)
			writeResults[i] = statuses
			for k := i; k < 256; k += writers {
				size := pressureWriteSize(lengths, k)
				cmd := target.makeCommand(pressureWriteKey(lengths, k), value[:size], size)
				started := time.Now()
				_, status := client.execute(cmd, 0, 0)
				statuses[status]++
				if status == ok || status == noSpace {
					writeLatency[i] = append(writeLatency[i], time.Since(started))
				}
			}
			if previous := client.conn; previous != nil {
				_, status := client.execute(hot[i], smallSize, 0)
				recovered[i] = status == ok && client.conn == previous
			}
		})
	}
	writes.Wait()
	writeEnd := time.Now()
	writeAfter := readRuntime(&httpClient)
	close(stopSampling)
	<-sampled
	workers.Wait()

	writeCounts := make(map[outcome]int)
	recoveredCount := 0
	var writeSamples []time.Duration
	for i, lane := range writeResults {
		writeSamples = append(writeSamples, writeLatency[i]...)
		for kind, n := range lane {
			writeCounts[kind] += n
		}
		if recovered[i] {
			recoveredCount++
		}
	}
	phases := map[string]*pressurePhase{
		"Before": {Reads: make(map[outcome]int)},
		"During": {Reads: make(map[outcome]int)},
		"After":  {Reads: make(map[outcome]int)},
	}
	timings := make(map[string][]time.Duration)
	for _, lane := range reads {
		for _, read := range lane {
			phase := "During"
			if read.at.Before(writeStart) {
				phase = "Before"
			} else if !read.at.Before(writeEnd) {
				phase = "After"
			}
			phases[phase].Reads[read.result]++
			if read.result == ok {
				timings[phase] = append(timings[phase], read.elapsed)
			}
		}
	}
	for phase, samples := range timings {
		slices.Sort(samples)
		phases[phase].P99Microseconds = percentile(samples, 990)
	}
	after, _ := diagnostics(target)
	result["Stats"] = after
	result["ReadSchedule"] = map[string]int{"Offered": scheduled.Offered, "Waiting": scheduled.Waiting, "Late": scheduled.Late}
	result["ReadPhases"] = phases
	slices.Sort(writeSamples)
	result["Writes"] = writeCounts
	result["WriteSamples"] = len(writeSamples)
	result["WriteP99Microseconds"] = percentile(writeSamples, 990)
	result["WriterConnectionsRecovered"] = recoveredCount
	result["WriteSeconds"] = writeEnd.Sub(writeStart).Seconds()
	result["RuntimeBefore"] = writeBefore
	result["RuntimeAfter"] = writeAfter
	result["RuntimeMax"] = peak
	return result
}

func readRuntime(client *http.Client) map[string]float64 {
	response, err := client.Get("http://127.0.0.1:9122/runtime")
	if err != nil {
		return nil
	}
	defer response.Body.Close()
	var result map[string]float64
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&result) != nil {
		return nil
	}
	return result
}

func pressureWriteSize(lengths string, index int) int {
	switch lengths {
	case "repeated":
		sizes := [...]int{256 << 10, 512 << 10, largeSize - 1, largeSize}
		return sizes[(index/8+index)%len(sizes)]
	case "adjacent":
		return largeSize - index
	case "publish":
		return largeSize - 1
	default:
		return largeSize
	}
}

func pressureWriteKey(lengths string, index int) string {
	if lengths == "publish" {
		index %= 2
	}
	return fmt.Sprintf("large%06d", index)
}
