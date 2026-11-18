//go:build unix

package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSIGTERMProcess(t *testing.T) {
	address := os.Getenv("MINIKV_TEST_SIGNAL_ADDRESS")
	if address == "" {
		return
	}
	os.Args = []string{"minikv", "-listen", address, "-inbound-bytes", "32"}
	main()
}

func TestSIGTERMWithPartialSet(t *testing.T) {
	cmd, address := startSignalProcess(t, "127.0.0.1:0")
	if got := signalRequest(t, address, "SET 1 3 NONE\nkold"); got != "STORED\n" {
		t.Fatalf("seed SET = %q", got)
	}

	// BUSY on a separate connection confirms the partial SET holds all inbound bytes.
	var pending net.Conn
	until := time.Now().Add(3 * time.Second)
	for pending == nil {
		conn := signalConn(t, address)
		if _, err := io.WriteString(conn, "SET 1 31 NONE\nkpart"); err != nil {
			t.Fatal(err)
		}
		switch got := signalRequest(t, address, "GET 1\nk"); got {
		case "BUSY\n":
			pending = conn
		case "VALUE 3\n":
			// Let the prior SET release its reservation before trying again.
			_ = conn.CloseWrite()
			_, _ = io.Copy(io.Discard, conn)
			conn.Close()
			if time.Now().After(until) {
				t.Fatal("partial SET never held the inbound budget")
			}
			time.Sleep(time.Millisecond)
		default:
			t.Fatalf("admission probe = %q", got)
		}
	}

	stopSignalProcess(t, cmd)
	var reply [1]byte
	if _, err := pending.Read(reply[:]); err != io.EOF {
		t.Fatalf("partial SET connection after SIGTERM: got %v, want EOF", err)
	}

	restarted, bound := startSignalProcess(t, address)
	if bound != address {
		t.Fatalf("restarted at %s, want %s", bound, address)
	}
	if got := signalRequest(t, address, "GET 1\nk"); got != "MISS\n" {
		t.Fatalf("GET after restart = %q, want MISS", got)
	}
	stopSignalProcess(t, restarted)
}

func startSignalProcess(t *testing.T, address string) (*exec.Cmd, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSIGTERMProcess$")
	cmd.Env = append(os.Environ(), "MINIKV_TEST_SIGNAL_ADDRESS="+address)
	output, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			cancel()
			_ = cmd.Wait()
		}
	})
	line, err := bufio.NewReader(output).ReadString('\n')
	bound, ok := strings.CutPrefix(line, "MiniKV listening on ")
	if err != nil || !ok {
		t.Fatalf("CLI startup = %q (%v)", line, err)
	}
	return cmd, strings.TrimSpace(bound)
}

func stopSignalProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("CLI exit after SIGTERM: %v", err)
	}
}

func signalConn(t *testing.T, address string) *net.TCPConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn.(*net.TCPConn)
}

func signalRequest(t *testing.T, address, frame string) string {
	t.Helper()
	conn := signalConn(t, address)
	defer conn.Close()
	if _, err := io.WriteString(conn, frame); err != nil {
		t.Fatal(err)
	}
	reply, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return reply
}
