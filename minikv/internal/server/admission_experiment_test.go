package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minikv/internal/store"
)

// TestAdmissionPeer is the TCP endpoint driven by experiments/admission.py.
func TestAdmissionPeer(t *testing.T) {
	config := store.Config{Shards: 1, BytesPerShard: 585728, EntriesPerShard: 512, Samples: 16}
	switch os.Getenv("MINIKV_ADMISSION_ARM") {
	case "two-fill":
		config.AdmissionWindow = 30 * time.Second
	case "direct":
	default:
		t.Skip("requires the admission experiment driver")
	}
	cache, err := store.New(config)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.ReadTimeout = time.Minute
	s, err := New(cache, cfg)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", ":7321")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()
	fmt.Println("MINIKV_ADMISSION_READY")
	if err := s.Serve(ctx, listener); err != nil {
		t.Fatal(err)
	}
}
