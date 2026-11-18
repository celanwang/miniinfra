package minimq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const (
	walFixtureCount = MaxBatchBytes / checkpointBodyBytes
	walFixtureMeta  = "test-wal-fixture.json"
	walCrashCode    = 75
)

var walFixtureTime = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

type walFixture struct {
	Ack      Delivery
	Reserved int64
}

func TestDeadLetterWALProcessExit(t *testing.T) {
	if dir := os.Getenv("MINIMQ_TEST_WAL_DIR"); dir != "" {
		walCrashChild(t, dir, os.Getenv("MINIMQ_TEST_WAL_MODE"))
		return
	}
	initial := t.TempDir()
	seedDeadLetterWAL(t, initial)
	before, err := os.ReadFile(filepath.Join(initial, "HEAD"))
	if err != nil || len(before) != 24 {
		t.Fatalf("initial HEAD has %d bytes, %v", len(before), err)
	}
	end := int64(binary.LittleEndian.Uint64(before[12:20]))
	generation := binary.LittleEndian.Uint64(before[4:12])
	for _, cut := range []string{"header", "wal-synced", "head-durable"} {
		t.Run(cut, func(t *testing.T) {
			dir := t.TempDir()
			copyWALFixture(t, initial, dir)
			out, err := runWALChild(os.Args[0], dir, cut)
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != walCrashCode {
				t.Fatalf("child missed write boundary: %v\n%s", err, out)
			}
			head, err := os.ReadFile(filepath.Join(dir, "HEAD"))
			if err != nil || len(head) != 24 {
				t.Fatalf("HEAD after exit has %d bytes, %v", len(head), err)
			}
			info, err := os.Stat(filepath.Join(dir, fmt.Sprintf("journal-%016x", generation)))
			if err != nil {
				t.Fatal(err)
			}
			recoverMode := "recover-unpublished"
			switch cut {
			case "header":
				if !bytes.Equal(head, before) || info.Size() != end+8 {
					t.Fatalf("header-only exit left %d extra bytes or changed HEAD", info.Size()-end)
				}
			case "wal-synced":
				if !bytes.Equal(head, before) || info.Size() <= end+MaxBatchBytes {
					t.Fatalf("unpublished full frame left %d extra bytes or changed HEAD", info.Size()-end)
				}
			case "head-durable":
				if bytes.Equal(head, before) || int64(binary.LittleEndian.Uint64(head[12:20])) != info.Size() {
					t.Fatal("the published HEAD does not cover the complete frame")
				}
				recoverMode = "recover-published"
			}
			recovery := os.Getenv("MINIMQ_WAL_RECOVERY_BINARY")
			if recovery == "" {
				recovery = os.Args[0]
			}
			if out, err := runWALChild(recovery, dir, recoverMode); err != nil {
				t.Fatalf("could not recover the partition: %v\n%s", err, out)
			}
		})
	}
}

func runWALChild(binary, dir, mode string) ([]byte, error) {
	cmd := exec.Command(binary, "-test.run=^TestDeadLetterWALProcessExit$")
	cmd.Env = append(os.Environ(), "MINIMQ_TEST_WAL_DIR="+dir, "MINIMQ_TEST_WAL_MODE="+mode)
	return cmd.CombinedOutput()
}

func copyWALFixture(t testing.TB, source, target string) {
	t.Helper()
	files, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		data, err := os.ReadFile(filepath.Join(source, file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(target, file.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func seedDeadLetterWAL(t *testing.T, dir string) {
	t.Helper()
	now := walFixtureTime
	p := openTest(t, dir, Config{Clock: testClock(&now), SegmentMessages: walFixtureCount})
	registerTest(t, p, "final", SubscriptionConfig{MaxAttempts: 1})
	for i := range walFixtureCount {
		req := PublishRequest{ID: fmt.Sprintf("final-%d", i), Body: bytes.Repeat([]byte{byte(i)}, checkpointBodyBytes)}
		if _, err := p.Publish(req); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	limit := Limits{Messages: walFixtureCount, Bytes: MaxBatchBytes}
	if ds, err := p.Pull("final", "worker", limit); err != nil || len(ds) != walFixtureCount {
		t.Fatalf("grant final leases = %d, %v", len(ds), err)
	}
	registerTest(t, p, "ordinary", SubscriptionConfig{})
	publishTest(t, p, "ack")
	ack := pullTest(t, p, "ordinary", "worker", 1)[0]
	ackTest(t, p, "ordinary", "worker", ack, AckAccepted)
	fixture, err := json.Marshal(walFixture{Ack: ack, Reserved: p.reserved})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, walFixtureMeta), fixture, 0o600); err != nil {
		t.Fatal(err)
	}
}

func walCrashChild(t *testing.T, dir, mode string) {
	t.Helper()
	now := walFixtureTime.Add(time.Minute)
	p := openTest(t, dir, Config{Clock: testClock(&now), SegmentMessages: walFixtureCount})
	limit := Limits{Messages: walFixtureCount, Bytes: MaxBatchBytes}
	switch mode {
	case "header", "wal-synced", "head-durable":
		if mode == "header" {
			p.journal.ObserveWrite = func(time.Duration) { os.Exit(walCrashCode) }
		} else {
			syncs := 0
			p.journal.ObserveSync = func(time.Duration) {
				syncs++
				if syncs == 3 || mode == "wal-synced" {
					os.Exit(walCrashCode)
				}
			}
		}
		n, err := p.AdvanceRetries(limit)
		t.Fatalf("expired %d leases without the requested interruption: %v", n, err)
	case "recover-unpublished", "recover-published":
		contents, err := os.ReadFile(filepath.Join(dir, walFixtureMeta))
		if err != nil {
			t.Fatal(err)
		}
		var meta walFixture
		if err := json.Unmarshal(contents, &meta); err != nil {
			t.Fatal(err)
		}
		released := int64(walFixtureCount) * (SubscriptionConfig{MaxAttempts: 1}.taskReserve(checkpointBodyBytes) - transitionReserve)
		wantBefore := 0
		wantReserved := meta.Reserved
		if mode == "recover-published" {
			wantBefore = walFixtureCount
			wantReserved -= released
		}
		checkWALDeadLetters(t, p, wantBefore)
		if p.reserved != wantReserved {
			t.Fatalf("reservation before resuming = %d, want %d", p.reserved, wantReserved)
		}
		if wantBefore == 0 {
			capacity, end := p.config.CapacityBytes, p.journal.End()
			p.config.CapacityBytes = 4096
			n, err := p.AdvanceRetries(limit)
			p.config.CapacityBytes = capacity
			if !errors.Is(err, ErrCapacity) || n != 0 || p.journal.End() != end || p.reserved != wantReserved {
				t.Fatalf("capacity rejection changed the committed frame or reservation: %d, %v", n, err)
			}
			checkWALDeadLetters(t, p, 0)
		}
		if n, err := p.AdvanceRetries(limit); err != nil || n != walFixtureCount-wantBefore {
			t.Fatalf("resumed expiry = %d, %v", n, err)
		}
		checkWALDeadLetters(t, p, walFixtureCount)
		if p.reserved != meta.Reserved-released {
			t.Fatalf("reservation after completing expiry = %d, want %d", p.reserved, meta.Reserved-released)
		}
		if n, err := p.AdvanceRetries(limit); err != nil || n != 0 {
			t.Fatalf("already settled expiry = %d, %v", n, err)
		}
		ackTest(t, p, "ordinary", "worker", meta.Ack, AckPreviouslyAccepted)
	default:
		t.Fatalf("unknown crash mode %q", mode)
	}
}

func checkWALDeadLetters(t *testing.T, p *Partition, want int) {
	t.Helper()
	letters, err := p.ListDeadLetters("final", "", MaxBatchMessages)
	if err != nil || len(letters) != want {
		t.Fatalf("dead letters = %d, %v, want %d", len(letters), err, want)
	}
	seen := make(map[uint64]bool, len(letters))
	for _, letter := range letters {
		offset := letter.Message.Offset
		if offset >= walFixtureCount || seen[offset] || letter.Attempts != 1 ||
			letter.Message.ID != fmt.Sprintf("final-%d", offset) ||
			!bytes.Equal(letter.Message.Body, bytes.Repeat([]byte{byte(offset)}, checkpointBodyBytes)) {
			t.Fatalf("unexpected dead letter for offset %d", offset)
		}
		seen[offset] = true
	}
}
