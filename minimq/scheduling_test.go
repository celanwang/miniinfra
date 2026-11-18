package minimq

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestScheduledPublicationRecoversWithOriginalMembership(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	width := time.Duration(0)
	cfg := Config{Clock: func() (TimeBounds, error) {
		return TimeBounds{Lower: now, Upper: now.Add(width)}, nil
	}}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "existing", SubscriptionConfig{})
	req := PublishRequest{ID: "delayed", Body: []byte("payload"), DeliverAt: now.Add(time.Hour)}
	original, err := p.Publish(req)
	if err != nil || !original.Scheduled {
		t.Fatalf("schedule = %v, %v", original, err)
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	registerTest(t, p, "later", SubscriptionConfig{})
	publishTest(t, p, "immediate")
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	if dup, err := p.Publish(req); err != nil || dup != original {
		t.Fatalf("retry pending publication = %v, %v", dup, err)
	}
	wrong := req
	wrong.DeliverAt = wrong.DeliverAt.Add(time.Second)
	if _, err := p.Publish(wrong); !errors.Is(err, ErrConflict) {
		t.Fatalf("schedule ID with different release time: %v", err)
	}
	now = req.DeliverAt.Add(-time.Nanosecond)
	width = 2 * time.Nanosecond
	if count, err := p.AdvanceScheduled(pageLimit); err != nil || count != 0 {
		t.Fatalf("early release = %d, %v", count, err)
	}
	now = req.DeliverAt
	if count, err := p.AdvanceScheduled(pageLimit); err != nil || count != 1 {
		t.Fatalf("release = %d, %v", count, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	if dup, err := p.Publish(req); err != nil || dup != original {
		t.Fatalf("retry released publication = %v, %v", dup, err)
	}
	if status, err := p.PublishStatus(req.ID); err != nil || status.Scheduled || status.Offset != 1 {
		t.Fatalf("released publication status = %v, %v", status, err)
	}
	if count, err := p.AdvanceScheduled(pageLimit); err != nil || count != 0 {
		t.Fatalf("duplicate release = %d, %v", count, err)
	}
	history, err := p.Read(0, pageLimit)
	if err != nil || history.Next != 2 || history.Messages[1].ID != "delayed" {
		t.Fatalf("unexpected history: %+v, %v", history, err)
	}
	pullTest(t, p, "existing", "worker", 2)
	later := pullTest(t, p, "later", "worker", 1)
	if later[0].Message.ID != "immediate" {
		t.Fatalf("later subscription received %+v", later[0])
	}
}

func TestCheckpointOrdersPendingSchedulesWithWAL(t *testing.T) {
	now := walFixtureTime
	dir := t.TempDir()
	cfg := Config{Clock: testClock(&now)}
	p := openTest(t, dir, cfg)
	shared := now.Add(2 * time.Minute)
	for _, req := range []PublishRequest{
		{ID: "z-first", DeliverAt: shared},
		{ID: "late", DeliverAt: shared.Add(time.Minute)},
		{ID: "early", DeliverAt: shared.Add(-time.Minute)},
		{ID: "a-second", DeliverAt: shared},
	} {
		if got, err := p.Publish(req); err != nil || !got.Scheduled {
			t.Fatalf("schedule %s = %+v, %v", req.ID, got, err)
		}
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if got, err := p.Publish(PublishRequest{ID: "m-third", DeliverAt: shared}); err != nil || !got.Scheduled {
		t.Fatalf("schedule in WAL = %+v, %v", got, err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	now = shared.Add(time.Minute)
	want := []string{"early", "z-first", "a-second", "m-third", "late"}
	if n, err := p.AdvanceScheduled(pageLimit); err != nil || n != len(want) {
		t.Fatalf("released %d, %v", n, err)
	}
	page, err := p.Read(0, pageLimit)
	if err != nil || len(page.Messages) != len(want) {
		t.Fatalf("history = %+v, %v", page, err)
	}
	for i, id := range want {
		if msg := page.Messages[i]; msg.ID != id || msg.Offset != uint64(i) {
			t.Fatalf("history at %d = %+v, want %s", i, msg, id)
		}
	}
}

func TestCheckpointScheduledIDsUseEmbeddedID(t *testing.T) {
	now := walFixtureTime
	dir := t.TempDir()
	cfg := Config{Clock: testClock(&now)}
	p := openTest(t, dir, cfg)
	req := PublishRequest{ID: "pending", Body: []byte("body"), DeliverAt: now.Add(time.Hour)}
	original, err := p.Publish(req)
	if err != nil || !original.Scheduled {
		t.Fatalf("publish = %+v, %v", original, err)
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	generation := binary.LittleEndian.Uint64(checkpointHEAD(t, dir)[4:12])
	path := filepath.Join(dir, fmt.Sprintf("checkpoint-%016x", generation))
	data, err := os.ReadFile(path)
	if err != nil || len(data) <= sha256.Size {
		t.Fatalf("read checkpoint: %d bytes, %v", len(data), err)
	}
	var image checkpoint
	if err := json.Unmarshal(data[:len(data)-sha256.Size], &image); err != nil {
		t.Fatal(err)
	}
	writeImage := func() {
		t.Helper()
		payload, err := json.Marshal(image)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(payload)
		if err := os.WriteFile(path, append(payload, digest[:]...), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	image.Scheduled["alias"] = image.Scheduled[req.ID]
	writeImage()
	if corrupt, err := Open(dir, cfg); !errors.Is(err, ErrCorrupt) {
		if corrupt != nil {
			_ = corrupt.Close()
		}
		t.Fatalf("two keys for the same scheduled ID reopened: %v", err)
	}
	delete(image.Scheduled, req.ID)
	writeImage()
	p = openTest(t, dir, cfg)
	if got, err := p.PublishStatus(req.ID); err != nil || got != original || len(p.scheduled) != 1 {
		t.Fatalf("embedded pending ID = %+v, %v", got, err)
	}
	now = req.DeliverAt
	if n, err := p.AdvanceScheduled(pageLimit); err != nil || n != 1 {
		t.Fatalf("release by embedded ID = %d, %v", n, err)
	}
}
