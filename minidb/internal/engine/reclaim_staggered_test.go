package engine_test

import (
	"context"
	"database/sql"
	"os"
	"runtime/metrics"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

func TestTCPStaggeredSnapshots(t *testing.T) {
	rows := 2800
	switch os.Getenv("MINIDB_SNAPSHOT_LOAD") {
	case "short":
		rows = 1200
	case "long":
	default:
		t.Skip("set MINIDB_SNAPSHOT_LOAD to short or long")
	}

	dir := t.TempDir()
	store, pool, closeStore := openTCP(t, dir)
	setupTables(t, pool, 12, rows)
	measureCtx, endMeasurement := context.WithCancel(t.Context())
	defer endMeasurement()
	sweeps := store.MeasureReclaimSweeps(measureCtx)

	started := time.Now()
	first := stockRead(t.Context(), pool, rows, false)
	var firstPin uint64
	until(t, "first SELECT pin", time.Second, func() bool {
		obs := store.ObserveReclamation()
		if len(obs.Snapshots) == 1 {
			firstPin = obs.Snapshots[0]
			return true
		}
		return false
	})

	ids := []int{1, 1, 1, 2, 3}
	load := tcpWriters{stop: make(chan struct{}), finished: make(chan writerResult, len(ids))}
	stop := sync.OnceFunc(func() { close(load.stop) })
	defer stop()
	for _, id := range ids {
		go load.start(t.Context(), pool, id, load.stop)
	}
	until(t, "four ordinary commits before starting the second SELECT", 2*time.Second, func() bool {
		return load.commits[2].Load() >= 4
	})
	stagger := time.Since(started)
	second := stockRead(t.Context(), pool, rows, true)

	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	var observations snapshotLoadSamples
	var checkpoint chan error
	var checkpointStarted bool
	var firstDuration, secondDuration time.Duration
	for first != nil || second != nil || checkpoint != nil {
		select {
		case result := <-first:
			firstDuration = time.Since(started)
			if result.err != nil || result.first != 0 || result.last != 0 {
				t.Fatalf("first SELECT lost its original view: %+v", result)
			}
			first = nil
		case result := <-second:
			secondDuration = time.Since(started)
			if result.err != nil || result.first == 0 || result.first != result.last {
				t.Fatalf("staggered SELECT lost its later view: %+v", result)
			}
			second = nil
		case err := <-checkpoint:
			if err != nil {
				t.Fatal(err)
			}
			checkpoint = nil
		case <-tick.C:
			if first == nil || second == nil {
				continue
			}
			obs := store.ObserveReclamation(writeTables...)
			if len(obs.Snapshots) < 2 || !slices.Contains(obs.Snapshots, firstPin) {
				continue
			}
			observations.record(t, obs)
			if !checkpointStarted {
				checkpointStarted = true
				result := make(chan error, 1)
				checkpoint = result
				go func() {
					_, err := pool.ExecContext(t.Context(), "CHECKPOINT")
					result <- err
				}()
			}
		}
	}
	if firstDuration <= stagger || observations.peak.Heap <= len(writeTables)*3 || !checkpointStarted {
		t.Fatalf("expected overlapping long queries, writes and CHECKPOINT: delay=%s first=%s peak=%+v",
			stagger, firstDuration, observations.peak)
	}

	readAttempts := load.totalAttempts()
	load.phase.Store(1)
	released := totalVersions(t, store.ObserveReclamation(writeTables...))
	after := time.Now()
	time.Sleep(750 * time.Millisecond)
	stop()
	var normal [2][]time.Duration
	for range ids {
		result := <-load.finished
		if result.err != nil {
			t.Fatal(result.err)
		}
		for _, sample := range result.samples {
			normal[sample.phase] = append(normal[sample.phase], sample.elapsed)
		}
	}
	if len(normal[0]) < 5 || len(normal[1]) < 5 {
		t.Fatalf("too few ordinary commits: during=%d after=%d", len(normal[0]), len(normal[1]))
	}
	writeRate := float64(load.totalAttempts()-readAttempts) * versionsPerTable * float64(len(writeTables)) / time.Since(after).Seconds()
	quiet := time.Now()
	until(t, "reclamation after both statements and writers stop", 15*time.Second, func() bool {
		obs := store.ObserveReclamation(writeTables...)
		return len(obs.Snapshots) == 0 && totalVersions(t, obs).Heap == 3*len(writeTables)
	})
	drain := time.Since(quiet)
	endMeasurement()
	work := <-sweeps

	t.Logf("rows=%d stagger=%s first_done=%s second_done=%s overlap_peak=%+v release=%+v",
		rows, stagger, firstDuration, secondDuration, observations.peak, released)
	t.Logf("overlap_version_bytes=%d overlap_go_live_peak=%d overlap_go_objects_peak=%d",
		observations.versionBytes, observations.liveBytes, observations.objectBytes)
	t.Logf("updates/s after=%.0f commits=%d+%d+%d rollbacks=%d ordinary_p99 overlap=%s (%d) after=%s (%d) drain=%s",
		writeRate, load.commits[1].Load(), load.commits[2].Load(), load.commits[3].Load(), load.rollbacks.Load(),
		p99(normal[0]), len(normal[0]), p99(normal[1]), len(normal[1]), drain)
	t.Logf("sweeps=%d scanned=%d removed=%d mean=%s max=%s",
		work.Steps, work.Scanned, work.Removed, work.Time/time.Duration(max(1, work.Steps)), work.Max)
	slow := work.Slowest
	remaining := work.Max - slow.Gate - slow.Prepare - slow.TableWait - slow.TableHold
	t.Logf("slowest_sweep_phases=%+v outside_phases=%s longest_table=%s hold=%s removal=%s",
		slow, remaining, work.MaxTableName, work.MaxTableHold, work.MaxTableRemove)

	testLockedReplacement(t, store, pool)
	closeStore()
	_, recovered, _ := openTCP(t, dir)
	for _, table := range writeTables {
		for id, request := range []string{"hot", "ordinary-a", "ordinary-b"} {
			want := load.commits[id+1].Load() * versionsPerTable
			if table == "stock_00" && id == 1 {
				want = 4242
			}
			for _, probe := range []struct {
				column string
				value  any
			}{{"id", id + 1}, {"req", request}} {
				if got := readQty(t, recovered, "SELECT qty FROM "+table+" WHERE "+probe.column+"=?", probe.value); got != want {
					t.Fatalf("recovered %s by %s = %d, want %d", table, probe.column, got, want)
				}
			}
		}
	}
}

func stockRead(ctx context.Context, pool *sql.DB, rows int, bySecondary bool) <-chan oldRead {
	read := make(chan oldRead, 1)
	go func() {
		const join = "SELECT h.qty,v.qty FROM stock_00 h JOIN slow_rows a ON 1=1 JOIN slow_rows b ON a.id>=? AND b.id>=? JOIN stock_03 v ON v.id=h.id"
		predicate := " WHERE h.id=2"
		if bySecondary {
			predicate = " WHERE h.req='ordinary-a'"
		}
		var result oldRead
		result.err = pool.QueryRowContext(ctx, join+predicate, rows, rows).Scan(&result.first, &result.last)
		read <- result
	}()
	return read
}

type snapshotLoadSamples struct {
	peak                                 engine.VersionObservation
	versionBytes, liveBytes, objectBytes uint64
}

func (p *snapshotLoadSamples) record(t *testing.T, obs engine.ReclaimObservation) {
	t.Helper()
	physical := totalVersions(t, obs)
	if physical.Heap > p.peak.Heap {
		p.peak = physical
	}
	p.versionBytes = max(p.versionBytes, physical.VersionBytes)
	samples := []metrics.Sample{{Name: "/gc/heap/live:bytes"}, {Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(samples)
	p.liveBytes = max(p.liveBytes, samples[0].Value.Uint64())
	p.objectBytes = max(p.objectBytes, samples[1].Value.Uint64())
}
