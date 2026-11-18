package minimq

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

const deadPageFixtureManifest = "dead-page-fixture.json"

// Materialize one reusable checkpoint, as with the backlog experiment.
// Normal delivery and timeout conversion are covered by the WAL and retry tests.
func TestSeedDeadLetterPageFixture(t *testing.T) {
	dir := os.Getenv("MINIMQ_DEAD_PAGE_SEED")
	if dir == "" {
		t.Skip("set MINIMQ_DEAD_PAGE_SEED to an empty fixture directory")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("fixture directory must exist and be empty: %v", err)
	}
	count := experimentInt(t, "MINIMQ_DEAD_PAGE_ROWS", 8192, 128, 32768)
	now := walFixtureTime
	p := openTest(t, dir, Config{Clock: testClock(&now)})
	groups := map[string][]string{}
	for _, name := range []string{"billing", "email", "analytics", "webhooks", "bulk"} {
		registerTest(t, p, name, SubscriptionConfig{MaxAttempts: 1})
		rows, size := max(128, count/8), 256
		switch name {
		case "billing":
			rows = count
		case "bulk":
			rows, size = 64, 64<<10
		}
		body := bytes.Repeat([]byte{'m'}, size)
		letters := make([]DeadLetter, 0, rows)
		for i := range rows {
			message := fmt.Sprintf("%s-%d", name, i)
			sum := sha256.Sum256([]byte(message))
			id := hex.EncodeToString(sum[:16])
			groups[name] = append(groups[name], id)
			letters = append(letters, DeadLetter{
				ID: id, Subscription: name, Instance: historyInstance(uint64(i)),
				Message:  Message{ID: message, Body: body, Offset: uint64(i), PublishedAt: now},
				Attempts: 1, Reason: "lease expired", DeadAt: now, Expires: now.Add(7 * 24 * time.Hour),
			})
		}
		if err := p.apply(transaction{Sequence: p.sequence + 1, Lower: now, Dead: letters}); err != nil {
			t.Fatal(err)
		}
		slices.Sort(groups[name])
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	manifest, err := json.Marshal(groups)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, deadPageFixtureManifest), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkDeadLetterPage(b *testing.B) {
	dir := os.Getenv("MINIMQ_DEAD_PAGE_FIXTURE")
	if dir == "" {
		b.Skip("set MINIMQ_DEAD_PAGE_FIXTURE to a seeded fixture directory")
	}
	data, err := os.ReadFile(filepath.Join(dir, deadPageFixtureManifest))
	if err != nil {
		b.Fatal(err)
	}
	var groups map[string][]string
	if err := json.Unmarshal(data, &groups); err != nil {
		b.Fatal(err)
	}
	now := walFixtureTime
	billing := groups["billing"]
	if len(billing) < 128 || len(groups["email"]) < 16 || len(groups["bulk"]) < 16 {
		b.Fatal("missing rows in the fixture manifest")
	}
	p, err := Open(dir, Config{Clock: testClock(&now)})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = p.Close() })
	for _, query := range []struct {
		name, subscription, after string
		limit                     int
	}{
		{"first", "billing", "", 16},
		{"middle", "billing", billing[len(billing)/2-1], 16},
		{"tail", "billing", billing[len(billing)-33], 16},
		{"other", "email", "", 16},
		{"max", "billing", "", MaxBatchMessages},
		{"bulk", "bulk", "", 16},
	} {
		b.Run(query.name, func(b *testing.B) {
			var selectIDs, clone, held time.Duration
			p.observe(func(_ string, s operationTiming) {
				selectIDs += s.page.selectIDs
				clone += s.page.clone
				held += s.finished.Sub(s.locked)
			})
			b.ReportAllocs()
			for b.Loop() {
				letters, err := p.ListDeadLetters(query.subscription, query.after, query.limit)
				if err != nil || len(letters) != min(query.limit, len(groups[query.subscription])) {
					b.Fatalf("page = %d, %v", len(letters), err)
				}
			}
			b.ReportMetric(float64(selectIDs.Nanoseconds())/float64(b.N), "select-ns/op")
			b.ReportMetric(float64(clone.Nanoseconds())/float64(b.N), "clone-ns/op")
			b.ReportMetric(float64(held.Nanoseconds())/float64(b.N), "held-ns/op")
		})
	}
}

func TestDeadLetterPagePreservesCursorRetentionAndBytePrefix(t *testing.T) {
	dir := t.TempDir()
	now := walFixtureTime
	expires := now.Add(time.Minute)
	cfg := Config{Clock: testClock(&now)}
	p := openTest(t, dir, cfg)
	registerTest(t, p, "first", SubscriptionConfig{})
	registerTest(t, p, "second", SubscriptionConfig{})
	large := bytes.Repeat([]byte{'m'}, MaxMessageBytes)
	letters := []DeadLetter{
		{ID: "00-expiring", Subscription: "first", Expires: expires},
		{ID: "02-other", Subscription: "second", Expires: expires.Add(time.Hour)},
		{ID: "09-over", Subscription: "first", Message: Message{Body: []byte("over")}, Expires: expires.Add(time.Hour)},
		{ID: "10-empty", Subscription: "first", Expires: expires.Add(time.Hour)},
	}
	for i := 1; i <= MaxBatchBytes/MaxMessageBytes; i++ {
		letters = append(letters, DeadLetter{
			ID: fmt.Sprintf("%02d-body", i), Subscription: "first", Message: Message{Body: large}, Expires: expires.Add(time.Hour),
		})
	}
	if err := p.apply(transaction{Sequence: p.sequence + 1, Lower: now, Dead: letters}); err != nil {
		t.Fatal(err)
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	now = expires.Add(-time.Nanosecond)
	first, err := p.ListDeadLetters("first", "", 1)
	if err != nil || len(first) != 1 || first[0].ID != "00-expiring" {
		t.Fatalf("page just before expiry = %d, %v", len(first), err)
	}
	now = expires
	check := func() {
		t.Helper()
		page, err := p.ListDeadLetters("first", "", 16)
		if err != nil || len(page) != 8 {
			t.Fatalf("byte-limited page = %d, %v", len(page), err)
		}
		for i, d := range page {
			if d.ID != fmt.Sprintf("%02d-body", i+1) || !bytes.Equal(d.Message.Body, large) {
				t.Fatalf("wrong letter at page position %d", i)
			}
		}
		page[0].Message.Body[0] = 'x'
		old, err := p.GetDeadLetter("first", "01-body")
		if err != nil || len(old.Message.Body) != MaxMessageBytes || old.Message.Body[0] != 'm' {
			t.Fatalf("page body mutated the stored letter: %v", err)
		}
		next, err := p.ListDeadLetters("first", "08-body", 3)
		if err != nil || len(next) != 2 || next[0].ID != "09-over" || next[1].ID != "10-empty" {
			t.Fatalf("next page = %d, %v", len(next), err)
		}
		between, err := p.ListDeadLetters("first", "08-z", 1)
		if err != nil || len(between) != 1 || between[0].ID != "09-over" {
			t.Fatalf("nonexistent cursor page = %d, %v", len(between), err)
		}
		isolated, err := p.ListDeadLetters("second", "", 1)
		if err != nil || len(isolated) != 1 || isolated[0].ID != "02-other" {
			t.Fatalf("other subscription page = %d, %v", len(isolated), err)
		}
		if last, err := p.ListDeadLetters("first", "10-empty", 16); err != nil || last != nil {
			t.Fatalf("end of pages = %d, %v", len(last), err)
		}
		if _, err := p.ListDeadLetters("first", "", MaxBatchMessages+1); !errors.Is(err, ErrInvalid) {
			t.Fatalf("oversized limit error = %v", err)
		}
		if _, err := p.ListDeadLetters("absent", "", 1); !errors.Is(err, ErrSubscription) {
			t.Fatalf("absent subscription error = %v", err)
		}
	}
	check()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p = openTest(t, dir, cfg)
	check()
}
