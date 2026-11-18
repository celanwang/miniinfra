package main

import (
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
)

func TestRunWithOccupiedAddress(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	address := occupied.Addr().String()

	if err := run(t.Context(), []string{"-listen", address}, io.Discard); !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("run with valid options: got %v, want address in use", err)
	}

	for _, tc := range []struct{ flag, value, want string }{
		{"-entries", "0", "invalid shard capacity or sample count"},
		{"-cache-bytes", "1", "shard byte budget must exceed metadata reservation"},
		{"-max-connections", "0", "invalid server configuration"},
	} {
		t.Run(tc.flag, func(t *testing.T) {
			args := []string{"-listen", address, tc.flag, tc.value}
			if err := run(t.Context(), args, io.Discard); err == nil || err.Error() != tc.want {
				t.Fatalf("run with %v: got %v, want %q", args, err, tc.want)
			}
		})
	}

	var help bytes.Buffer
	if err := run(t.Context(), []string{"-listen", address, "-help"}, &help); err != nil ||
		!strings.Contains(help.String(), "Usage of minikv:") {
		t.Fatalf("run with -help: got %v, output %q", err, help.String())
	}
}
