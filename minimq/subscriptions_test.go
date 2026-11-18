package minimq

import (
	"errors"
	"testing"
	"time"
)

func registerTest(t *testing.T, p *Partition, name string, config SubscriptionConfig) {
	t.Helper()
	if err := p.Register(name, config); err != nil {
		t.Fatal(err)
	}
}

func pullTest(t *testing.T, p *Partition, name, worker string, count int) []Delivery {
	t.Helper()
	d, err := p.Pull(name, worker, pageLimit)
	if err != nil || len(d) != count {
		t.Fatalf("pull %s: %v, %v, want %d records", name, d, err, count)
	}
	return d
}

func ackTest(t *testing.T, p *Partition, name, worker string, delivery Delivery, want AckResult) {
	t.Helper()
	got, err := p.Ack(name, worker, delivery.Instance, delivery.Token)
	if err != nil || got != want {
		t.Fatalf("ack = %q, %v, want %q", got, err, want)
	}
}

func TestLeaseRecoveryAndLateAck(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := Config{Clock: testClock(&now)}
	p := openTest(t, dir, cfg)
	publishTest(t, p, "before-register")
	registerTest(t, p, "notify", SubscriptionConfig{LeaseDuration: time.Minute})
	publishTest(t, p, "notice")
	old := pullTest(t, p, "notify", "a", 1)[0]
	if old.Message.ID != "notice" || old.Attempt != 1 {
		t.Fatalf("unexpected first delivery: %+v", old)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	pullTest(t, p, "notify", "b", 0)
	now = old.Deadline
	if _, err := p.Ack("notify", "a", old.Instance, old.Token); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("ACK at lease deadline: %v", err)
	}
	fresh := pullTest(t, p, "notify", "b", 1)[0]
	if old.Token == fresh.Token || fresh.Attempt != 2 {
		t.Fatalf("unexpected new lease: %+v", fresh)
	}
	if _, err := p.Ack("notify", "a", old.Instance, old.Token); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("ACK with stale token: %v", err)
	}
	ackTest(t, p, "notify", "b", fresh, AckAccepted)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	now = fresh.Deadline.Add(time.Minute)
	ackTest(t, p, "notify", "b", fresh, AckPreviouslyAccepted)
	pullTest(t, p, "notify", "a", 0)
}

func TestAckWindowAndUncertainClock(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	width := time.Duration(0)
	cfg := Config{Clock: func() (TimeBounds, error) {
		return TimeBounds{Lower: now, Upper: now.Add(width)}, nil
	}}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "notify", SubscriptionConfig{LeaseDuration: time.Minute, Window: 2})
	for _, id := range []string{"one", "two", "three"} {
		publishTest(t, p, id)
	}
	d := pullTest(t, p, "notify", "a", 2)
	ackTest(t, p, "notify", "a", d[1], AckAccepted)
	now = d[0].Deadline.Add(-time.Nanosecond)
	width = 2 * time.Nanosecond
	if _, err := p.Ack("notify", "a", d[0].Instance, d[0].Token); !errors.Is(err, ErrClockUncertain) {
		t.Fatalf("ACK at uncertain deadline: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	pullTest(t, p, "notify", "b", 0)
	width = 0
	ackTest(t, p, "notify", "a", d[0], AckAccepted)
	got := pullTest(t, p, "notify", "b", 1)
	if got[0].Message.ID != "three" {
		t.Fatalf("unexpected delivery after window opened: %+v", got)
	}
	now = now.Add(-time.Second)
	if _, err := p.Ack("notify", "b", got[0].Instance, got[0].Token); !errors.Is(err, ErrClockUncertain) {
		t.Fatalf("ACK after clock rollback: %v", err)
	}
}

func TestReopenWithUnavailableClockPreservesLeasesAndAckReceipts(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	untrusted := false
	cfg := Config{Clock: func() (TimeBounds, error) {
		if untrusted {
			return TimeBounds{}, errors.New("clock provider lost its error bound")
		}
		return TimeBounds{Lower: now, Upper: now}, nil
	}}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "notify", SubscriptionConfig{LeaseDuration: time.Minute})
	publishTest(t, p, "first")
	publishTest(t, p, "second")
	deliveries := pullTest(t, p, "notify", "a", 2)
	ackTest(t, p, "notify", "a", deliveries[0], AckAccepted)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	untrusted = true
	p = openTest(t, dir, cfg)
	ackTest(t, p, "notify", "a", deliveries[0], AckPreviouslyAccepted)
	if _, err := p.Ack("notify", "a", deliveries[1].Instance, deliveries[1].Token); !errors.Is(err, ErrClockUncertain) {
		t.Fatalf("new ACK with an untrusted clock: %v", err)
	}
	if _, err := p.AdvanceRetries(pageLimit); !errors.Is(err, ErrClockUncertain) {
		t.Fatalf("expiry with an untrusted clock: %v", err)
	}
	if got, err := p.Read(0, pageLimit); err != nil || len(got.Messages) != 2 {
		t.Fatalf("history with an untrusted clock: %+v, %v", got, err)
	}
	untrusted = false
	pullTest(t, p, "notify", "b", 0)
	ackTest(t, p, "notify", "a", deliveries[1], AckAccepted)
}
