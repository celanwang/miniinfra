// Command load replays a fixed-rate GET/SET workload through the TCP service.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	ordinary   = 88
	background = 8
	smallSize  = 512
	largeSize  = 1 << 20
	smallKeys  = 2048
	largeKeys  = 2
	grace      = 5 * time.Millisecond
)

type command struct {
	frame []byte
	reply string
	set   bool
}

type outcome string

const (
	ok      outcome = "ok"
	miss    outcome = "miss"
	exists  outcome = "exists"
	busy    outcome = "busy"
	noSpace outcome = "no_space"
	timeout outcome = "timeout"
	wire    outcome = "wire"
)

type target struct {
	address string
	redis   bool
}

func (t target) makeCommand(key string, value []byte, size int) command {
	set := value != nil
	if !t.redis {
		if !set {
			return command{fmt.Appendf(nil, "GET %d\n%s", len(key), key), fmt.Sprintf("VALUE %d\n", size), false}
		}
		header := fmt.Appendf(make([]byte, 0, len(value)+128), "SET %d %d NONE\n%s", len(key), len(value), key)
		return command{append(header, value...), "STORED\n", true}
	}
	if !set {
		return command{fmt.Appendf(nil, "*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key), fmt.Sprintf("$%d\r\n", size), false}
	}
	header := fmt.Appendf(make([]byte, 0, len(value)+128), "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n", len(key), key, len(value))
	return command{append(append(header, value...), '\r', '\n'), "+OK\r\n", true}
}

type client struct {
	conn    *net.TCPConn
	reader  *bufio.Reader
	target  target
	limited bool
}

func (c *client) close() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.reader = nil
	}
}

func (c *client) connect() error {
	conn, err := net.DialTimeout("tcp4", c.target.address, 200*time.Millisecond)
	if err != nil {
		return err
	}
	c.conn = conn.(*net.TCPConn)
	if c.limited {
		if err := c.conn.SetReadBuffer(64 << 10); err != nil {
			c.close()
			return err
		}
	}
	c.reader = bufio.NewReaderSize(c.conn, 4096)
	return nil
}

func (c *client) execute(cmd command, size int, pause time.Duration) (bool, outcome) {
	if c.conn == nil {
		if err := c.connect(); err != nil {
			return false, errorOutcome(err)
		}
	}
	err := c.conn.SetDeadline(time.Now().Add(time.Second))
	if err == nil {
		_, err = c.conn.Write(cmd.frame)
	}
	if err != nil {
		c.close()
		return false, errorOutcome(err)
	}
	if pause > 0 {
		time.Sleep(pause)
	}
	line, err := c.reader.ReadSlice('\n')
	if err != nil {
		c.close()
		return true, errorOutcome(err)
	}
	switch {
	case string(line) == cmd.reply:
		if !cmd.set {
			if c.target.redis {
				size += 2 // RESP adds CRLF after the value.
			}
			if _, err := io.CopyN(io.Discard, c.reader, int64(size)); err != nil {
				c.close()
				return true, errorOutcome(err)
			}
		}
		return true, ok
	case string(line) == "EXISTS\n":
		return true, exists
	case string(line) == "MISS\n" || string(line) == "$-1\r\n":
		return true, miss
	case string(line) == "NO_SPACE\n" || bytes.HasPrefix(line, []byte("-OOM ")):
		return true, noSpace
	case string(line) == "BUSY\n":
		// Inbound BUSY can close the connection with the body still unread.
		if cmd.set {
			c.close()
		}
		return true, busy
	default:
		c.close()
		return true, wire
	}
}

func errorOutcome(err error) outcome {
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return timeout
	}
	return wire
}

type entry struct {
	get command
	set command
}

type scenario struct {
	small []entry
	large []entry
}

func prepare(t target) scenario {
	sizes := []int{smallSize, largeSize}
	tables := make([][]entry, len(sizes))
	for i, n := range []int{smallKeys, largeKeys} {
		value := bytes.Repeat([]byte("v"), sizes[i])
		tables[i] = make([]entry, n)
		for k := range tables[i] {
			key := fmt.Sprintf("item%d-%05d", i, k)
			tables[i][k] = entry{t.makeCommand(key, nil, len(value)), t.makeCommand(key, value, len(value))}
		}
	}
	return scenario{tables[0], tables[1]}
}

type sample struct {
	online []time.Duration
	rtt    []time.Duration
}

type counts struct {
	Offered   int
	Sent      int
	Waiting   int
	Late      int
	Responses map[outcome]int
}

type result struct {
	Ordinary            counts
	Background          counts
	SmallGETs           int
	P99Microseconds     float64
	P999Microseconds    float64
	RTTP99Microseconds  float64
	RTTP999Microseconds float64
	ClientCPUSeconds    float64
	ServerCPUTicks      int64
	ServerWriteCalls    *int64 `json:",omitempty"`
}

type lane struct {
	client     *client
	load       scenario
	background bool
	profile    string
}

type arrival struct {
	sequence int
	at       time.Time
}

func (l lane) run(queue <-chan arrival) (counts, sample) {
	res := counts{Responses: make(map[outcome]int)}
	var timings sample
	for job := range queue {
		entry, size, set, pause := l.pick(job.sequence)
		cmd := entry.get
		if set {
			cmd = entry.set
		}
		began := time.Now()
		sent, response := l.client.execute(cmd, size, pause)
		end := time.Now()
		if sent {
			res.Sent++
		}
		res.Responses[response]++
		if !l.background && !set && response == ok {
			timings.online = append(timings.online, end.Sub(job.at))
			timings.rtt = append(timings.rtt, end.Sub(began))
		}
	}
	return res, timings
}

func schedule(start time.Time, window, tick time.Duration, rate int, ordinaryQ, backgroundQ chan arrival, ordinary, background *counts) {
	nextSmall, nextBackground := 0, 0
	for elapsed := time.Duration(0); elapsed < window; elapsed += tick {
		at := start.Add(elapsed)
		time.Sleep(time.Until(at) - min(300*time.Microsecond, tick))
		for time.Now().Before(at) {
		}
		late := time.Since(at) > grace
		through := min(elapsed+tick, window)
		nextSmall = offer(ordinaryQ, ordinary, at, late, nextSmall, int(through*time.Duration(rate)/time.Second))
		nextBackground = offer(backgroundQ, background, at, late, nextBackground, int(through*40/time.Second))
	}
	close(ordinaryQ)
	close(backgroundQ)
}

func offer(queue chan arrival, count *counts, at time.Time, late bool, next, limit int) int {
	for sequence := next; sequence < limit; sequence++ {
		count.Offered++
		if late {
			count.Late++
			continue
		}
		select {
		case queue <- arrival{sequence, at}:
		default:
			count.Waiting++
		}
	}
	return limit
}

func (l lane) pick(index int) (entry, int, bool, time.Duration) {
	// Multiplication and xor decorrelate the hot and write decisions while
	// keeping each lane's offered sequence identical across engines.
	mixed := uint64(index+1) * 11400714819323198485
	seed := mixed ^ (mixed >> 31)
	if l.background && l.profile != "small" {
		if (seed>>16)%10 == 0 {
			return l.load.large[seed%largeKeys], largeSize, true, 0
		}
		pause := time.Duration(0)
		if l.profile == "slow" {
			pause = 120 * time.Millisecond
		}
		return l.load.large[seed%largeKeys], largeSize, false, pause
	}
	key := (seed >> 24) % smallKeys
	if seed%5 != 0 {
		key %= 8
	}
	return l.load.small[key], smallSize, (seed>>17)%20 == 0, 0
}

func percentile(times []time.Duration, perThousand int) float64 {
	if len(times) == 0 {
		return 0
	}
	i := (len(times)*perThousand+999)/1000 - 1
	return float64(times[i]) / float64(time.Microsecond)
}

func runCurve(t target, profile string, rate int, window, tick time.Duration) (result, error) {
	load := prepare(t)
	warm := client{target: t}
	defer warm.close()
	for _, table := range [][]entry{load.small, load.large} {
		for _, item := range table {
			if _, response := warm.execute(item.set, 0, 0); response != ok {
				return result{}, fmt.Errorf("prefill: %s", response)
			}
		}
	}
	warm.close()
	lanes := make([]lane, ordinary+background)
	for i := range lanes {
		c := &client{target: t, limited: i >= ordinary}
		defer c.close()
		if err := c.connect(); err != nil {
			return result{}, err
		}
		lanes[i] = lane{client: c, load: load, profile: profile, background: i >= ordinary}
	}
	return runLanes(lanes, rate, window, tick), nil
}

func runLanes(lanes []lane, rate int, window, tick time.Duration) result {
	aggregate := result{
		Ordinary:   counts{Responses: make(map[outcome]int)},
		Background: counts{Responses: make(map[outcome]int)},
	}
	laneCounts := make([]counts, len(lanes))
	timings := make([]sample, len(lanes))
	start := time.Now().Add(200 * time.Millisecond)
	before := cpuSeconds()
	serverBefore := serverTicks()
	writesBefore, writesAvailable := serverWriteCalls()
	ordinaryQ := make(chan arrival, 256)
	backgroundQ := make(chan arrival, background)
	var wg sync.WaitGroup
	for i, l := range lanes {
		wg.Go(func() {
			queue := ordinaryQ
			if l.background {
				queue = backgroundQ
			}
			laneCounts[i], timings[i] = l.run(queue)
		})
	}
	schedule(start, window, tick, rate, ordinaryQ, backgroundQ, &aggregate.Ordinary, &aggregate.Background)
	wg.Wait()
	aggregate.ClientCPUSeconds = cpuSeconds() - before
	aggregate.ServerCPUTicks = serverTicks() - serverBefore
	if writesAfter, ok := serverWriteCalls(); writesAvailable && ok {
		calls := writesAfter - writesBefore
		aggregate.ServerWriteCalls = &calls
	}
	var online, rtt []time.Duration
	for i, from := range laneCounts {
		to := &aggregate.Ordinary
		if lanes[i].background {
			to = &aggregate.Background
		}
		to.Offered += from.Offered
		to.Sent += from.Sent
		to.Waiting += from.Waiting
		to.Late += from.Late
		for outcome, n := range from.Responses {
			to.Responses[outcome] += n
		}
		online = append(online, timings[i].online...)
		rtt = append(rtt, timings[i].rtt...)
	}
	slices.Sort(online)
	slices.Sort(rtt)
	aggregate.SmallGETs = len(online)
	aggregate.P99Microseconds = percentile(online, 990)
	aggregate.P999Microseconds = percentile(online, 999)
	aggregate.RTTP99Microseconds = percentile(rtt, 990)
	aggregate.RTTP999Microseconds = percentile(rtt, 999)
	return aggregate
}

func cpuSeconds() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		log.Fatal(err)
	}
	return float64(usage.Utime.Nano()+usage.Stime.Nano()) / 1e9
}

// The driver joins the cache container's PID namespace. PID 1 is the server.
func serverTicks() int64 {
	data, err := os.ReadFile("/proc/1/stat")
	if err != nil {
		log.Fatal(err)
	}
	_, rest, ok := strings.Cut(string(data), ") ")
	fields := strings.Fields(rest)
	if !ok || len(fields) < 13 {
		log.Fatal("invalid server process stat")
	}
	user, err := strconv.ParseInt(fields[11], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	system, err := strconv.ParseInt(fields[12], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return user + system
}

func serverWriteCalls() (int64, bool) {
	data, err := os.ReadFile("/proc/1/io")
	if err != nil {
		return 0, false
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if count, ok := strings.CutPrefix(line, "syscw:"); ok {
			calls, err := strconv.ParseInt(strings.TrimSpace(count), 10, 64)
			return calls, err == nil
		}
	}
	return 0, false
}

func diagnostics(t target) (json.RawMessage, error) {
	if t.redis {
		return json.RawMessage("null"), nil
	}
	client := http.Client{Timeout: time.Second}
	response, err := client.Get("http://127.0.0.1:9122/stats")
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("diagnostics HTTP %s", response.Status)
	}
	var stats json.RawMessage
	err = json.NewDecoder(response.Body).Decode(&stats)
	return stats, err
}

func fill(t target, prefix string, clients, n, size int) map[outcome]int {
	return fillKeys(t, clients, n, size, func(i int) string { return fmt.Sprintf("%s%06d", prefix, i) })
}

func fillKeys(t target, clients, n, size int, key func(int) string) map[outcome]int {
	value := bytes.Repeat([]byte("v"), size)
	partial := make([]map[outcome]int, clients)
	var workers sync.WaitGroup
	for i := range clients {
		workers.Go(func() {
			c := client{target: t}
			defer c.close()
			partial[i] = make(map[outcome]int)
			for k := i; k < n; k += clients {
				cmd := t.makeCommand(key(k), value, size)
				_, result := c.execute(cmd, 0, 0)
				partial[i][result]++
			}
		})
	}
	workers.Wait()
	result := make(map[outcome]int)
	for _, totals := range partial {
		for status, n := range totals {
			result[status] += n
		}
	}
	return result
}

func main() {
	redis := flag.Bool("redis", false, "use Redis RESP2")
	profile := flag.String("profile", "small", "small, mixed, slow, capacity, or pressure")
	rate := flag.Int("rate", 10000, "ordinary offered requests per second")
	window := flag.Duration("duration", 6*time.Second, "offered workload duration")
	tick := flag.Duration("tick", time.Millisecond, "fixed arrival batch interval")
	lengths := flag.String("pressure-lengths", "fixed", "pressure write size and locality pattern")
	blockProfile := flag.Bool("block-profile", false, "fetch a Go block profile from the fixture")
	ttlPattern := flag.String("ttl-pattern", "clustered", "clustered, spread, byte-fit, or byte-limit TTL expiry")
	flag.Parse()
	t := target{address: "127.0.0.1:7321", redis: *redis}
	encoder := json.NewEncoder(os.Stdout)
	if *profile == "ttl" && !*redis {
		var result map[string]any
		switch *ttlPattern {
		case "clustered", "spread":
			result = runTTL(t, *ttlPattern)
		case "byte-fit", "byte-limit":
			result = runTTLBytes(t, *ttlPattern)
		default:
			log.Fatal("invalid TTL pattern")
		}
		if err := encoder.Encode(result); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *profile == "pressure" && !*redis {
		switch *lengths {
		case "fixed", "repeated", "adjacent", "publish", "publish-locality", "publish-aligned",
			"large-to-half", "half-to-large", "cold-half", "alternating", "business":
		default:
			log.Fatal("invalid pressure length pattern")
		}
		if err := encoder.Encode(runPressure(t, *lengths, *blockProfile)); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *profile == "capacity" {
		prefill := fill(t, "fill", 32, 100000, 1024)
		fmt.Fprintln(os.Stderr, "capacity prefill finished:", prefill)
		before, err := diagnostics(t)
		if err != nil {
			log.Fatal(err)
		}
		started := time.Now()
		cpu := serverTicks()
		writes := fill(t, "large", 8, 256, largeSize)
		elapsed := time.Since(started)
		cpu = serverTicks() - cpu
		after, err := diagnostics(t)
		if err != nil {
			log.Fatal(err)
		}
		if err := encoder.Encode(map[string]any{"Prefill": prefill, "Writes": writes, "Before": before, "Stats": after, "WriteSeconds": elapsed.Seconds(), "WriteServerCPUTicks": cpu}); err != nil {
			log.Fatal(err)
		}
		return
	}
	if (*profile != "small" && *profile != "mixed" && *profile != "slow") || *rate <= 0 || *window <= 0 || *tick <= 0 {
		log.Fatal("invalid profile, rate, or duration")
	}
	load, err := runCurve(t, *profile, *rate, *window, *tick)
	if err != nil {
		log.Fatal(err)
	}
	stats, err := diagnostics(t)
	if err != nil {
		log.Fatal(err)
	}
	if err := encoder.Encode(map[string]any{"Load": load, "Stats": stats}); err != nil {
		log.Fatal(err)
	}
}
