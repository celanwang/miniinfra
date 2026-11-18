package minimq

import (
	"errors"
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
