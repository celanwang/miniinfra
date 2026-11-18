package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minimq"
)

func TestRetiringTailCheckpointOverTCP(t *testing.T) {
	dir := os.Getenv("MINIMQ_TAIL_RETIRE_TCP_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("TCP fixture directory must be empty: %v", err)
	}
	var age atomic.Int64
	cfg := minimq.Config{
		SegmentMessages: 2, HistoryRetention: time.Minute,
		Clock: func() (minimq.TimeBounds, error) {
			now := dueExperimentEpoch.Add(time.Duration(age.Load()))
			return minimq.TimeBounds{Lower: now, Upper: now}, nil
		},
	}
	p, err := minimq.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	server := httptest.NewServer(routes(p))
	t.Cleanup(server.Close)
	client := server.Client()
	client.Timeout = 5 * time.Second
	for i := range 5 {
		if i == 2 {
			age.Store(int64(cfg.HistoryRetention))
		}
		payload := fmt.Sprintf("body-%d", i)
		result := requireDuePost[minimq.PublishResult](t, client, server.URL, "/v1/publish", minimq.PublishRequest{ID: payload, Body: []byte(payload)})
		if result.Offset != uint64(i) {
			t.Fatalf("publication %d took offset %d", i, result.Offset)
		}
	}
	before := requireDuePost[minimq.HistoryPage](t, client, server.URL, "/v1/history/read", historyRequest{
		Limit: minimq.Limits{Messages: 5, Bytes: 128},
	})
	if len(before.Messages) != 5 || before.Next != 5 || string(before.Messages[0].Body) != "body-0" {
		t.Fatalf("history before the checkpoint = %+v", before)
	}
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	verifyRetiredTailOverTCP(t, client, server.URL)
	server.Close()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	reopenRetiredTailOverTCP(t, dir)
}

// Allows a test binary built with an older broker to check the same directory.
func TestReadRetiredTailWithBroker(t *testing.T) {
	dir := os.Getenv("MINIMQ_TAIL_RETIRE_TCP_VERIFY_DIR")
	if dir == "" {
		t.Skip("set MINIMQ_TAIL_RETIRE_TCP_VERIFY_DIR to a completed TCP fixture")
	}
	reopenRetiredTailOverTCP(t, dir)
}

func reopenRetiredTailOverTCP(t *testing.T, dir string) {
	t.Helper()
	broker := startDueBroker(t, dir, time.Now().Add(-time.Minute))
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	verifyRetiredTailOverTCP(t, client, broker.url)
}

func verifyRetiredTailOverTCP(t *testing.T, client *http.Client, url string) {
	t.Helper()
	res, err := client.Post(url+"/v1/history/read", "application/json",
		strings.NewReader(`{"Offset":0,"Limit":{"Messages":1,"Bytes":128}}`))
	if err != nil {
		t.Fatal(err)
	}
	var gap errorResponse
	err = json.NewDecoder(res.Body).Decode(&gap)
	res.Body.Close()
	if err != nil || res.StatusCode != http.StatusGone || gap.Code != "offset_gap" || gap.First == nil || *gap.First != 2 {
		t.Fatalf("reclaimed history returned HTTP %d, %+v, %v", res.StatusCode, gap, err)
	}
	page := requireDuePost[minimq.HistoryPage](t, client, url, "/v1/history/read", historyRequest{
		Offset: 2, Limit: minimq.Limits{Messages: 4, Bytes: 128},
	})
	if page.Next != 5 || len(page.Messages) != 3 {
		t.Fatalf("retained TCP page = %+v", page)
	}
	for i, msg := range page.Messages {
		offset := i + 2
		payload := fmt.Sprintf("body-%d", offset)
		if msg.Offset != uint64(offset) || msg.ID != payload || string(msg.Body) != payload {
			t.Fatalf("retained TCP record at %d = %+v", offset, msg)
		}
	}
}
