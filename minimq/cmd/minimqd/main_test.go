package main

import (
	"bufio"
	"context"
	"errors"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minimq"
)

func TestMinimqdStartupAndRestart(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "minimqd")
	if output, err := exec.CommandContext(t.Context(), "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build minimqd: %v\n%s", err, output)
	}
	occupied := listenLoopback(t, "127.0.0.1:0")
	fresh := filepath.Join(root, "fresh")
	rejectMinimqdStartup(t, binary, fresh, occupied.Addr().String(), "unexpected positional argument", "extra")
	rejectMinimqdStartup(t, binary, fresh, occupied.Addr().String(), "address already in use")
	if _, err := os.Stat(fresh); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed startup touched the new partition directory: %v", err)
	}

	dir := filepath.Join(root, "live")
	broker, address := startMinimqdProcess(t, binary, dir, "127.0.0.1:0")
	client := &http.Client{Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	url := "http://" + address
	const sub, worker = "ordinary", "worker"
	requireDuePost[struct{}](t, client, url, "/v1/subscriptions/register", registerRequest{Subscription: sub})
	request := minimq.PublishRequest{ID: "message", Body: []byte("durable body")}
	receipt := requireDuePost[minimq.PublishResult](t, client, url, durablePublishPath, request)
	if want := (minimq.PublishResult{ID: request.ID}); receipt != want {
		t.Fatalf("publish = %+v, want %+v", receipt, want)
	}
	limit := minimq.Limits{Messages: 1, Bytes: 128}
	pull := pullRequest{Subscription: sub, Worker: worker, Limit: limit}
	deliveries := requireDuePost[[]minimq.Delivery](t, client, url, durablePullPath, pull)
	if len(deliveries) != 1 || deliveries[0].Message.ID != request.ID || string(deliveries[0].Message.Body) != string(request.Body) {
		t.Fatalf("pull before shutdown = %+v", deliveries)
	}
	ack := deliveryRequest{Subscription: sub, Worker: worker, Instance: deliveries[0].Instance, Token: deliveries[0].Token}
	if got := requireDuePost[minimq.AckResult](t, client, url, durableAckPath, ack); got != minimq.AckAccepted {
		t.Fatalf("ACK before shutdown = %q", got)
	}
	committed := partitionFiles(t, dir)
	rejectMinimqdStartup(t, binary, dir, "127.0.0.1:0", "lock partition")
	if !maps.Equal(committed, partitionFiles(t, dir)) {
		t.Fatal("second process changed the open partition")
	}
	broker.terminate(t)
	client.CloseIdleConnections()

	// Simulate a stale staged file and WAL bytes beyond HEAD. A failed bind
	// must leave both in place for the next successful recovery.
	wals, err := filepath.Glob(filepath.Join(dir, "journal-*"))
	if err != nil || len(wals) != 1 {
		t.Fatalf("find committed WAL: %v, %v", wals, err)
	}
	wal := wals[0]
	if err := os.WriteFile(wal, []byte(committed[filepath.Base(wal)]+"uncommitted frame tail"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".minimq-unpublished"), []byte("stale stage"), 0o600); err != nil {
		t.Fatal(err)
	}
	withTail := partitionFiles(t, dir)
	rejectMinimqdStartup(t, binary, dir, occupied.Addr().String(), "address already in use")
	if !maps.Equal(withTail, partitionFiles(t, dir)) {
		t.Fatal("failed bind changed the existing partition")
	}

	broker, reopened := startMinimqdProcess(t, binary, dir, address)
	if reopened != address || !maps.Equal(committed, partitionFiles(t, dir)) {
		t.Fatal("same-address restart did not recover the committed files")
	}
	if got := requireDuePost[minimq.PublishResult](t, client, url, durablePublishPath+"/status", idRequest{ID: request.ID}); got != receipt {
		t.Fatalf("reopened publish status = %+v, want %+v", got, receipt)
	}
	if got := requireDuePost[minimq.PublishResult](t, client, url, durablePublishPath, request); got != receipt {
		t.Fatalf("repeated publish after restart = %+v, want %+v", got, receipt)
	}
	if got := requireDuePost[minimq.AckResult](t, client, url, durableAckPath, ack); got != minimq.AckPreviouslyAccepted {
		t.Fatalf("recovered ACK = %q", got)
	}
	page := requireDuePost[minimq.HistoryPage](t, client, url, "/v1/history/read", historyRequest{Limit: limit})
	if len(page.Messages) != 1 || page.Next != 1 || page.Messages[0].Offset != receipt.Offset ||
		page.Messages[0].ID != request.ID || string(page.Messages[0].Body) != string(request.Body) {
		t.Fatalf("reopened history = %+v", page)
	}
	if got := requireDuePost[[]minimq.Delivery](t, client, url, durablePullPath, pull); len(got) != 0 {
		t.Fatalf("reopened subscription redelivered the ACKed message: %+v", got)
	}
	broker.terminate(t)
}

func TestRunReleasesListenerAndLockAfterRecoveryFailure(t *testing.T) {
	dir := t.TempDir()
	p, err := minimq.Open(dir, minimq.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	head := filepath.Join(dir, "HEAD")
	data, err := os.ReadFile(head)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(head, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	listener := listenLoopback(t, "127.0.0.1:0")
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := run(ctx, dir, address, minimq.Config{}); !errors.Is(err, minimq.ErrCorrupt) {
		t.Fatalf("start on corrupt HEAD = %v", err)
	}
	listenLoopback(t, address)
	if err := os.WriteFile(head, data, 0o600); err != nil {
		t.Fatal(err)
	}
	p, err = minimq.Open(dir, minimq.Config{})
	if err != nil {
		t.Fatalf("reopen original HEAD: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func listenLoopback(t *testing.T, address string) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func partitionFiles(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := make(map[string]string, len(entries))
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[entry.Name()] = string(data)
	}
	return files
}

func rejectMinimqdStartup(t *testing.T, binary, dir, address, reason string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, append([]string{"-dir", dir, "-listen", address}, args...)...)
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), reason) || ctx.Err() != nil {
		t.Fatalf("startup failure = %v, want %q\n%s", err, reason, output)
	}
}

type minimqdProcess struct {
	command *exec.Cmd
	done    chan struct{}
	err     error
	logs    string
}

func startMinimqdProcess(t *testing.T, binary, dir, address string) (*minimqdProcess, string) {
	t.Helper()
	command := exec.Command(binary, "-dir", dir, "-listen", address)
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &minimqdProcess{command: command, done: make(chan struct{})}
	ready := make(chan string, 1)
	go func() {
		var logs strings.Builder
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			logs.WriteString(line + "\n")
			if _, address, found := strings.Cut(line, "MiniMQ listening on "); found {
				ready <- address
			}
		}
		process.logs = logs.String()
		process.err = errors.Join(scanner.Err(), command.Wait())
		close(process.done)
	}()
	t.Cleanup(func() {
		select {
		case <-process.done:
			return
		default:
		}
		_ = command.Process.Kill()
		<-process.done
	})
	select {
	case address := <-ready:
		return process, address
	case <-process.done:
		t.Fatalf("minimqd exited before listening: %v\n%s", process.err, process.logs)
	case <-time.After(8 * time.Second):
		t.Fatal("minimqd did not start listening")
	}
	return nil, ""
}

func (p *minimqdProcess) terminate(t *testing.T) {
	t.Helper()
	if err := p.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
		if p.err != nil {
			t.Fatalf("minimqd did not exit cleanly after SIGTERM: %v\n%s", p.err, p.logs)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("minimqd did not exit after SIGTERM")
	}
}
