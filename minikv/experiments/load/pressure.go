package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

type pressureRead struct {
	at      time.Time
	elapsed time.Duration
	rtt     time.Duration
	result  outcome
}

type pressurePhase struct {
	Reads              map[outcome]int
	P99Microseconds    float64
	RTTP99Microseconds float64
}

const (
	pressureWriters        = 8
	evictionWritesPerShard = 192
	evictionEntryBytes     = (4<<20)/ttlShardSlots - 112
)

func evictionWriteClass(step int) int {
	if step < 120 {
		return 0
	}
	if step%3 == 0 {
		return 2 // Seventeen entries exceed the sample budget of sixteen.
	}
	return 1
}

func runPressure(target target, lengths string, blockProfile bool) map[string]any {
	result := make(map[string]any)
	publish := strings.HasPrefix(lengths, "publish")
	locality := lengths == "publish-locality" || lengths == "publish-aligned"
	var business *businessPressure
	var eviction [][]ttlEntry
	hotSize := smallSize
	if lengths == "eviction" {
		var err error
		eviction, err = ttlLayout(ttlShardSlots, evictionWritesPerShard)
		if err != nil {
			result["SetupError"] = err.Error()
			return result
		}
		hotSize = evictionEntryBytes - len(eviction[0][0].key)
		value := bytes.Repeat([]byte("r"), hotSize)
		prefill := populateTTL(target, eviction, ttlShardSlots, func(int) ([]byte, time.Time) { return value, time.Time{} })
		result["Prefill"] = prefill
		if prefill[ok] != ttlShards*ttlShardSlots {
			return result
		}
	} else if lengths == "business" {
		var err error
		business, err = prepareBusinessPressure(target)
		if err != nil {
			result["SetupError"] = err.Error()
			return result
		}
		result["Prefill"] = business.prefill
	} else if !publish {
		prefill := fill(target, "fill", 32, 100000, 1024)
		result["Prefill"] = prefill
		if prefill[ok] != 100000 {
			return result
		}
	}

	const readers = 16
	const writers = pressureWriters
	hot := make([]command, readers)
	warm := client{target: target}
	defer warm.close()
	small := bytes.Repeat([]byte("r"), smallSize)
	var keys []string
	if locality {
		var err error
		keys, err = pressureReadKeys(pressureWriteKey(lengths, 0), readers)
		if err != nil {
			result["RoutingError"] = err.Error()
			return result
		}
	}
	for i := range hot {
		if eviction != nil {
			hot[i] = target.makeCommand(eviction[i][3500].key, nil, hotSize)
			continue
		}
		key := fmt.Sprintf("read%06d", i)
		if locality {
			key = keys[i]
		}
		if _, status := warm.execute(target.makeCommand(key, small, smallSize), 0, 0); status != ok {
			result["WarmReadKeyError"] = status
			return result
		}
		hot[i] = target.makeCommand(key, nil, smallSize)
	}
	if publish {
		payload := bytes.Repeat([]byte("p"), pressureWriteSize(lengths, 0))
		keyCount := 2
		if locality {
			keyCount = 1
		}
		for i := range keyCount {
			cmd := target.makeCommand(pressureWriteKey(lengths, i), payload, len(payload))
			if _, status := warm.execute(cmd, 0, 0); status != ok {
				result["WarmWriteKeyError"] = status
				return result
			}
		}
	}
	if business != nil {
		if err := business.seedSuccessKeys(&warm); err != nil {
			result["SetupError"] = err.Error()
			return result
		}
	}
	warm.close()
	before, _ := diagnostics(target)
	result["Before"] = before

	var workers sync.WaitGroup
	reads := make([][]pressureRead, readers)
	queue := make(chan arrival, 256)
	readQueues := [2]<-chan arrival{queue, queue}
	var routingDrops int
	if locality {
		same, cross := make(chan arrival, 128), make(chan arrival, 128)
		readQueues = [2]<-chan arrival{same, cross}
		go func() {
			destinations := [2]chan arrival{same, cross}
			for job := range queue {
				select {
				case destinations[job.sequence%2] <- job:
				default:
					routingDrops++
				}
			}
			close(same)
			close(cross)
		}()
	}
	for i := range readers {
		workers.Go(func() {
			client := client{target: target}
			defer client.close()
			for job := range readQueues[i/(readers/2)] {
				started := time.Now()
				_, status := client.execute(hot[i], hotSize, 0)
				end := time.Now()
				reads[i] = append(reads[i], pressureRead{job.at, end.Sub(job.at), end.Sub(started), status})
			}
		})
	}
	start := time.Now().Add(100 * time.Millisecond)
	var scheduled, auxiliary counts
	go schedule(start, 2*time.Second, 250*time.Microsecond, 10000, 0, queue, make(chan arrival, 8), &scheduled, &auxiliary)

	httpClient := http.Client{Timeout: 40 * time.Millisecond}
	time.Sleep(time.Until(start.Add(250 * time.Millisecond)))
	var primeStart, primeEnd time.Time
	if size := switchPrimeSize(lengths); size != 0 {
		primingBefore := readRuntime(&httpClient)
		finishPrime := samplePressureRuntime(&httpClient, primingBefore)
		primeStart = time.Now()
		prime := primeWritePool(target, hot, size)
		primeEnd = time.Now()
		prime["Before"] = primingBefore
		primeAfter := readRuntime(&httpClient)
		prime["After"] = primeAfter
		prime["Peak"] = finishPrime(primeAfter)
		result["Prime"] = prime
	}
	writeBefore := readRuntime(&httpClient)
	finishSampling := samplePressureRuntime(&httpClient, writeBefore)
	writeStart := time.Now()
	writeResults := make([]map[outcome]int, writers)
	writeLatency := make([][]time.Duration, writers)
	var evictionLatency [writers][3][]time.Duration
	recovered := make([]bool, writers)
	verified := make([]bool, writers)
	unexpected := make([]int, writers)
	value := bytes.Repeat([]byte("v"), largeSize)
	var writes sync.WaitGroup
	for i := range writers {
		writes.Go(func() {
			client := client{target: target}
			defer client.close()
			statuses := make(map[outcome]int)
			writeResults[i] = statuses
			data := value
			if business != nil {
				data = bytes.Clone(value)
			}
			var lastPublished []byte
			for k := i; k < pressureWriteCount(lengths); k += writers {
				size := pressureWriteSize(lengths, k)
				key := pressureWriteKey(lengths, k)
				var expected outcome
				var writeClass int
				if business != nil {
					key, size, expected = business.write(k)
					if expected == ok {
						data[0] = byte(k / writers)
					}
				}
				if eviction != nil {
					step := k / writers
					at := writeStart.Add(time.Duration(step*writers+i) * 250 * time.Microsecond)
					time.Sleep(time.Until(at))
					key = eviction[i][ttlShardSlots+step].key
					writeClass = evictionWriteClass(step)
					size = [...]int{1, 2, 17}[writeClass]*evictionEntryBytes - len(key)
					expected = [...]outcome{ok, ok, noSpace}[writeClass]
				}
				cmd := target.makeCommand(key, data[:size], size)
				started := time.Now()
				_, status := client.execute(cmd, 0, 0)
				elapsed := time.Since(started)
				statuses[status]++
				if expected != "" && expected != status {
					unexpected[i]++
				}
				if business != nil {
					if expected == ok && status == ok {
						lastPublished = data[:size]
					}
				}
				if eviction != nil && status == expected {
					evictionLatency[i][writeClass] = append(evictionLatency[i][writeClass], elapsed)
				}
				if status == ok || status == noSpace {
					writeLatency[i] = append(writeLatency[i], elapsed)
				}
			}
			if previous := client.conn; previous != nil {
				_, status := client.execute(hot[i], hotSize, 0)
				recovered[i] = status == ok && client.conn == previous
				if business != nil && recovered[i] && len(lastPublished) > 0 {
					verified[i] = business.verify(&client, i, lastPublished) && client.conn == previous
				}
			}
		})
	}
	writes.Wait()
	writeEnd := time.Now()
	writeAfter := readRuntime(&httpClient)
	peak := finishSampling(writeAfter)
	workers.Wait()
	if !primeStart.IsZero() {
		result["PrimeReads"] = pressureReadPhases(reads, primeStart, primeEnd)["During"]
	}

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
	phases := pressureReadPhases(reads, writeStart, writeEnd)
	if locality || eviction != nil {
		result["SameShard"] = pressureReadPhases(reads[:readers/2], writeStart, writeEnd)
		result["CrossShard"] = pressureReadPhases(reads[readers/2:], writeStart, writeEnd)
	}
	if blockProfile {
		result["BlockProfile"] = readBlockProfile()
	}
	after, _ := diagnostics(target)
	result["Stats"] = after
	result["ReadSchedule"] = map[string]int{"Offered": scheduled.Offered, "Waiting": scheduled.Waiting + routingDrops, "Late": scheduled.Late}
	result["ReadPhases"] = phases
	slices.Sort(writeSamples)
	result["Writes"] = writeCounts
	result["WriteSamples"] = len(writeSamples)
	result["WriteP99Microseconds"] = percentile(writeSamples, 990)
	result["WriterConnectionsRecovered"] = recoveredCount
	if eviction != nil {
		classResults := make(map[string]any)
		for class, name := range [...]string{"Single", "Multiple", "Reject"} {
			var times []time.Duration
			for _, lane := range evictionLatency {
				times = append(times, lane[class]...)
			}
			classResults[name] = map[string]any{"Count": len(times), "Latency": describePageLatency(times)}
		}
		result["EvictionWrites"] = classResults
	}
	if business != nil || eviction != nil {
		var failures, verifiedCount int
		for i, n := range unexpected {
			failures += n
			if verified[i] {
				verifiedCount++
			}
		}
		result["UnexpectedWrites"] = failures
		if business != nil {
			result["VerifiedValues"] = verifiedCount
		}
	}
	result["WriteSeconds"] = writeEnd.Sub(writeStart).Seconds()
	result["RuntimeBefore"] = writeBefore
	result["RuntimeAfter"] = writeAfter
	result["RuntimeMax"] = peak
	return result
}

func pressureReadPhases(reads [][]pressureRead, writeStart, writeEnd time.Time) map[string]*pressurePhase {
	phases := map[string]*pressurePhase{
		"Before": {Reads: make(map[outcome]int)},
		"During": {Reads: make(map[outcome]int)},
		"After":  {Reads: make(map[outcome]int)},
	}
	timings := make(map[string][]time.Duration)
	rtts := make(map[string][]time.Duration)
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
				rtts[phase] = append(rtts[phase], read.rtt)
			}
		}
	}
	for phase, samples := range timings {
		slices.Sort(samples)
		phases[phase].P99Microseconds = percentile(samples, 990)
		slices.Sort(rtts[phase])
		phases[phase].RTTP99Microseconds = percentile(rtts[phase], 990)
	}
	return phases
}

func pressureReadKeys(writeKey string, readers int) ([]string, error) {
	keys := make([]string, 1025)
	keys[0] = writeKey
	for i := 1; i < len(keys); i++ {
		keys[i] = fmt.Sprintf("read%06d", i)
	}
	routes, err := pressureKeyRoutes(keys)
	if err != nil {
		return nil, err
	}
	var same, cross []string
	crossShard := (routes[0] + 1) % 16
	for i, route := range routes[1:] {
		if route == routes[0] && len(same) < readers/2 {
			same = append(same, keys[i+1])
		}
		if route == crossShard && len(cross) < readers/2 {
			cross = append(cross, keys[i+1])
		}
	}
	if len(same)+len(cross) != readers {
		return nil, fmt.Errorf("could not choose read keys for shards %d and %d", routes[0], crossShard)
	}
	return append(same, cross...), nil
}

func pressureKeyRoutes(keys []string) ([]int, error) {
	body, err := json.Marshal(keys)
	if err != nil {
		return nil, err
	}
	c := &http.Client{Timeout: time.Second}
	resp, err := c.Post("http://127.0.0.1:9122/shards", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var routes []int
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&routes) != nil || len(routes) != len(keys) {
		return nil, fmt.Errorf("invalid routes from fixture")
	}
	return routes, nil
}

func readBlockProfile() string {
	c := &http.Client{Timeout: time.Second}
	resp, err := c.Get("http://127.0.0.1:9122/debug/pprof/block?debug=1")
	if err != nil {
		return err.Error()
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
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
	case "large-to-half", "cold-half":
		return largeSize / 2
	case "alternating":
		if index/8%2 != 0 {
			return largeSize / 2
		}
		return largeSize
	case "publish", "publish-locality":
		return largeSize - 1
	default:
		return largeSize
	}
}

func pressureWriteKey(lengths string, index int) string {
	switch lengths {
	case "publish":
		index %= 2
	case "publish-locality", "publish-aligned":
		index = 0
	}
	return fmt.Sprintf("large%06d", index)
}
