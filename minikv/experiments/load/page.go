package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	pageObjects     = 16
	pageConnections = 16
	singleConns     = 16
	singleRate      = 10000
	pageDefaultKey  = 12
)

type pageObject struct {
	get   command
	value []byte
}

type pageLane struct {
	counts
	times    sample
	requests []time.Duration
	sent     int
	peak     int
}

type pageLatency struct {
	P50US float64
	P99US float64
}

func describePageLatency(times []time.Duration) pageLatency {
	slices.Sort(times)
	return pageLatency{percentile(times, 500), percentile(times, 990)}
}

func runPage(t target, depth, rate, keyBytes int, window, tick time.Duration) (map[string]any, error) {
	var objects [pageObjects]pageObject
	prefix := "page-item-"
	if keyBytes != pageDefaultKey {
		const namespace = "tenant:000042:catalog:region:cn:product:detail:variant:public:"
		prefix = strings.Repeat(namespace, (keyBytes+len(namespace)-1)/len(namespace))[:keyBytes-2]
	}
	warm := client{target: t}
	defer warm.close()
	for index := range objects {
		key := fmt.Sprintf("%s%02d", prefix, index)
		value := bytes.Repeat([]byte{byte(index + 1)}, smallSize)
		if _, status := warm.execute(t.makeCommand(key, value, len(value)), 0, 0); status != ok {
			return nil, fmt.Errorf("page prefill: %s", status)
		}
		objects[index] = pageObject{t.makeCommand(key, nil, len(value)), value}
	}
	warm.close()
	clients := make([]client, pageConnections+singleConns)
	for i := range clients {
		clients[i].target = t
		defer clients[i].close()
		if err := clients[i].connect(); err != nil {
			return nil, err
		}
	}

	pages, singles := make(chan arrival, 256), make(chan arrival, 256)
	var pageOffered, singleOffered counts
	lanes := make([]pageLane, len(clients))
	var wg sync.WaitGroup
	admin := &http.Client{Timeout: time.Second}
	runtimeBefore := readRuntime(admin)
	start := time.Now().Add(200 * time.Millisecond)
	for i := range clients {
		wg.Go(func() {
			c := &clients[i]
			l := &lanes[i]
			l.Responses = make(map[outcome]int)
			if i < pageConnections {
				for job := range pages {
					began := time.Now()
					sent, peak, status := c.getPage(objects[:], depth, &l.requests)
					l.sent += sent
					l.peak = max(l.peak, peak)
					l.Responses[status]++
					if sent > 0 {
						l.Sent++
					}
					if status == ok {
						complete := time.Now()
						l.times.online = append(l.times.online, complete.Sub(job.at))
						l.times.rtt = append(l.times.rtt, complete.Sub(began))
					}
				}
				return
			}
			for job := range singles {
				began := time.Now()
				sent, status := c.execute(objects[i%pageObjects].get, smallSize, 0)
				l.Responses[status]++
				if sent {
					l.Sent++
					l.peak = 1
				}
				if status == ok {
					complete := time.Now()
					l.times.online = append(l.times.online, complete.Sub(job.at))
					l.times.rtt = append(l.times.rtt, complete.Sub(began))
				}
			}
		})
	}
	schedule(start, window, tick, rate, singleRate, pages, singles, &pageOffered, &singleOffered)
	wg.Wait()
	runtimeAfter := readRuntime(admin)

	pageOffered.Responses = make(map[outcome]int)
	singleOffered.Responses = make(map[outcome]int)
	var pageTimes, singleTimes sample
	var requests []time.Duration
	var requestSent, peak, singlePeak int
	for i, lane := range lanes {
		c, timings := &singleOffered, &singleTimes
		if i < pageConnections {
			c, timings = &pageOffered, &pageTimes
			requests = append(requests, lane.requests...)
			requestSent += lane.sent
			peak = max(peak, lane.peak)
		} else {
			singlePeak = max(singlePeak, lane.peak)
		}
		c.Sent += lane.Sent
		for status, n := range lane.Responses {
			c.Responses[status] += n
		}
		timings.online = append(timings.online, lane.times.online...)
		timings.rtt = append(timings.rtt, lane.times.rtt...)
	}
	return map[string]any{
		"PageConnections": pageConnections, "SingleConnections": singleConns, "ObjectsPerPage": pageObjects,
		"Pages": pageOffered, "Singles": singleOffered, "MaxPendingPerPageConn": peak, "MaxPendingPerSingleConn": singlePeak,
		"PageGETsSent": requestSent, "PageGETsVerified": len(requests), "PageGETLatency": describePageLatency(requests),
		"PageOnline": describePageLatency(pageTimes.online), "PageActive": describePageLatency(pageTimes.rtt),
		"SingleOnline": describePageLatency(singleTimes.online), "SingleRTT": describePageLatency(singleTimes.rtt),
		"RuntimeBefore": runtimeBefore, "RuntimeAfter": runtimeAfter,
	}, nil
}

func (c *client) getPage(objects []pageObject, depth int, times *[]time.Duration) (sent, peak int, status outcome) {
	if c.conn == nil {
		if err := c.connect(); err != nil {
			return 0, 0, errorOutcome(err)
		}
	}
	defer func() {
		if status != ok {
			c.close()
		}
	}()
	if err := c.conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		return 0, 0, errorOutcome(err)
	}
	var payload [smallSize]byte
	var started [4]time.Time
	for start := 0; start < len(objects); start += depth {
		batch := objects[start : start+depth]
		for i, object := range batch {
			started[i] = time.Now()
			if _, err := c.conn.Write(object.get.frame); err != nil {
				return sent, peak, errorOutcome(err)
			}
			sent++
			peak = max(peak, i+1)
		}
		for i, object := range batch {
			line, err := c.reader.ReadSlice('\n')
			if err != nil {
				return sent, peak, errorOutcome(err)
			}
			switch string(line) {
			case "MISS\n":
				return sent, peak, miss
			case "BUSY\n":
				return sent, peak, busy
			case object.get.reply:
				if _, err := io.ReadFull(c.reader, payload[:]); err != nil {
					return sent, peak, errorOutcome(err)
				}
				if !bytes.Equal(payload[:], object.value) {
					return sent, peak, wire
				}
				*times = append(*times, time.Since(started[i]))
			default:
				return sent, peak, wire
			}
		}
	}
	return sent, peak, ok
}
