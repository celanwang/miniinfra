package main

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"time"
)

func switchPrimeSize(lengths string) int {
	switch lengths {
	case "large-to-half":
		return largeSize
	case "half-to-large":
		return largeSize / 2
	default:
		return 0
	}
}

func pressureWriteCount(lengths string) int {
	switch lengths {
	case "large-to-half", "cold-half":
		return 512
	case "alternating":
		return 384
	default:
		return 256
	}
}

func samplePressureRuntime(client *http.Client, initial map[string]float64) func(map[string]float64) map[string]float64 {
	peak := make(map[string]float64, len(initial))
	for key, value := range initial {
		peak[key] = value
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				for key, value := range readRuntime(client) {
					peak[key] = max(peak[key], value)
				}
			}
		}
	}()
	return func(final map[string]float64) map[string]float64 {
		close(stop)
		<-done
		for key, value := range final {
			peak[key] = max(peak[key], value)
		}
		return peak
	}
}

// Hold each legal frame one byte short so all large receive buffers are
// checked out concurrently before the server can reject and return any.
func primeWritePool(target target, hot []command, size int) map[string]any {
	const budget = 8 << 20
	writers := budget / size
	results := make(map[string]any)
	statuses := make(map[outcome]int)
	results["Writes"] = statuses
	results["SizeBytes"] = size
	results["Writers"] = writers

	payload := bytes.Repeat([]byte("p"), size)
	clients := make([]client, writers)
	expected := 0
	for i := range clients {
		c := &clients[i]
		c.target = target
		defer c.close()
		if err := c.connect(); err != nil {
			results["Error"] = err.Error()
			return results
		}
		key := fmt.Sprintf("prime%06d", i)
		cmd := target.makeCommand(key, payload, size)
		expected += len(key) + size
		_ = c.conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := c.conn.Write(cmd.frame[:len(cmd.frame)-1]); err != nil {
			results["Error"] = err.Error()
			return results
		}
	}

	httpClient := http.Client{Timeout: 40 * time.Millisecond}
	deadline := time.Now().Add(time.Second)
	for readRuntime(&httpClient)["InboundBytes"] < float64(expected) {
		if time.Now().After(deadline) {
			results["Error"] = "priming buffers did not enter the server together"
			return results
		}
		time.Sleep(time.Millisecond)
	}

	outcomes := make([]outcome, writers)
	recovered := make([]bool, writers)
	var workers sync.WaitGroup
	for i := range clients {
		workers.Go(func() {
			c := &clients[i]
			if _, err := c.conn.Write(payload[size-1:]); err != nil {
				outcomes[i] = errorOutcome(err)
				return
			}
			line, err := c.reader.ReadString('\n')
			switch {
			case err != nil:
				outcomes[i] = errorOutcome(err)
			case line == "NO_SPACE\n":
				outcomes[i] = noSpace
			default:
				outcomes[i] = wire
			}
			if previous := c.conn; outcomes[i] == noSpace {
				_, status := c.execute(hot[i%len(hot)], smallSize, 0)
				recovered[i] = status == ok && c.conn == previous
			}
		})
	}
	workers.Wait()
	count := 0
	for i, recorded := range outcomes {
		statuses[recorded]++
		if recovered[i] {
			count++
		}
	}
	results["WriterConnectionsRecovered"] = count
	return results
}
