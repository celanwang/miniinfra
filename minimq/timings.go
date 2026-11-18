package minimq

import "time"

type operationTiming struct {
	arrived, locked, finished time.Time
	syncTime                  time.Duration
	syncCalls                 int
	worker                    string
	checkpoint                checkpointTiming
	retry                     retryTiming
	commit                    commitTiming
	page                      deadLetterPageTiming
}

type deadLetterPageTiming struct {
	selectIDs, clone time.Duration
}

type checkpointTiming struct {
	prepare, encode, frame, write, cleanup, cleanupSync time.Duration
	tailFirst                                           uint64
	tailRecords, tailBytes, imageBytes, stagedSegments  int
}

type retryTiming struct {
	scan, order, read, decode, prepare, commit time.Duration
	visited, candidates, settled, dead         int
	deadBytes                                  int
}

type commitTiming struct {
	encode, frame, write time.Duration
	bytes                int
}

// An optional observer, installed before opening the workload to concurrent
// calls. Its counters are updated under the partition lock.
type partitionTimings struct {
	record           func(string, operationTiming)
	time             time.Duration
	writeTime        time.Duration
	calls            int
	checkpoint       checkpointTiming
	retry            retryTiming
	commit           commitTiming
	checkpointLocked func()
}

func (p *Partition) observe(record func(string, operationTiming)) {
	timings := &partitionTimings{record: record}
	p.timings = timings
	p.journal.ObserveSync = func(elapsed time.Duration) {
		timings.time += elapsed
		timings.calls++
	}
	p.journal.ObserveWrite = func(elapsed time.Duration) {
		timings.writeTime += elapsed
		timings.checkpoint.write += elapsed
	}
}

func (p *Partition) beginOperation() operationTiming {
	if p.timings == nil {
		p.mu.Lock()
		return operationTiming{}
	}
	arrived := time.Now()
	p.mu.Lock()
	p.timings.retry = retryTiming{}
	p.timings.commit = commitTiming{}
	return operationTiming{
		arrived: arrived, locked: time.Now(), syncTime: p.timings.time, syncCalls: p.timings.calls,
	}
}

func (p *Partition) endOperation(operation string, sample operationTiming) {
	if timing := p.timings; timing != nil {
		sample.finished = time.Now()
		sample.syncTime = timing.time - sample.syncTime
		sample.syncCalls = timing.calls - sample.syncCalls
		if operation == "checkpoint" {
			sample.checkpoint = timing.checkpoint
		}
		sample.retry = timing.retry
		sample.commit = timing.commit
		timing.record(operation, sample)
	}
	p.mu.Unlock()
}

func checkpointStart(stats *checkpointTiming) time.Time {
	if stats != nil {
		return time.Now()
	}
	return time.Time{}
}
