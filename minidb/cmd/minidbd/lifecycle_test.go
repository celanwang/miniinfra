package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/client"
)

// Re-exec the test binary to exercise the real CLI and signal handler.
func TestDaemonCLIProcess(t *testing.T) {
	if os.Getenv("MINIDB_TEST_RUN_CLI") != "1" {
		return
	}
	os.Args = append(os.Args[:1], flag.Args()...)
	flag.CommandLine = flag.NewFlagSet("minidbd", flag.ExitOnError)
	main()
}

func daemonCommand(ctx context.Context, t *testing.T, dir string, args ...string) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, exe, append([]string{"-test.run=^TestDaemonCLIProcess$", "--"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "MINIDB_TEST_RUN_CLI=1")
	return cmd
}

func TestListenFailureLeavesDatabaseUntouched(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	start := func(t *testing.T, dir string) {
		t.Helper()
		if err := run(t.Context(), occupied.Addr().String(), dir); !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatalf("starting on occupied port: %v", err)
		}
	}
	t.Run("empty_directory", func(t *testing.T) {
		dir := t.TempDir()
		start(t, dir)
		assertEmptyDataDir(t, dir)
	})
	t.Run("existing_checkpoint_and_torn_wal", func(t *testing.T) {
		dir := t.TempDir()
		addr, stop := launch(t, dir)
		db := client.Open(addr)
		defer db.Close()
		execute(t, db, "create table seed (id int primary key)")
		execute(t, db, "insert into seed values (1)")
		execute(t, db, "checkpoint")
		stop()

		pages := readDatabaseFile(t, dir, "minidb.pages")
		tornWAL := append(readDatabaseFile(t, dir, "minidb.wal"), []byte("torn")...)
		writeDatabaseFile(t, dir, "minidb.wal", tornWAL)
		start(t, dir)
		if wal := readDatabaseFile(t, dir, "minidb.wal"); !bytes.Equal(wal, tornWAL) {
			t.Fatalf("failed listener startup changed the WAL from %d to %d bytes", len(tornWAL), len(wal))
		}
		if got := readDatabaseFile(t, dir, "minidb.pages"); !bytes.Equal(got, pages) {
			t.Fatal("failed listener startup changed the checkpoint")
		}
	})
}

func TestOpenFailureReleasesListener(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if err := run(t.Context(), addr, filepath.Join(t.TempDir(), "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("opening missing data directory: %v", err)
	}
	listener, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed startup retained listener: %v", err)
	}
	listener.Close()
}

func TestUnexpectedPositionalArgumentDoesNotOpenDefaultDirectory(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := daemonCommand(ctx, t, dir, "-listen", "127.0.0.1:0", "unexpected")
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 2 ||
		!strings.Contains(string(output), "unexpected positional arguments") {
		t.Fatalf("unexpected argument exit: %v\n%s", err, output)
	}
	assertEmptyDataDir(t, dir)
}

func assertEmptyDataDir(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("failed startup left database files: %v", entries)
	}
}

func TestSIGTERMRecoversOnlyCommittedOrders(t *testing.T) {
	dir := t.TempDir()
	cmd := daemonCommand(t.Context(), t, dir, "-listen", "127.0.0.1:0", "-data", dir)
	addr, _, wait := launchCommand(t, cmd)
	db := client.Open(addr)
	defer db.Close()
	setup(t, db)
	execute(t, db, "insert into inventory values (3,10),(4,20)")
	if err := reserveOrder(t, db).Commit(); err != nil {
		t.Fatal(err)
	}
	const id, sku, request = 43, 3, "open-at-shutdown"
	pending := reserveCheckpointOrder(t, db, id, request, sku)
	var available int
	if err := pending.QueryRowContext(t.Context(), "select available from inventory where sku = ?", sku).Scan(&available); err != nil || available != 8 {
		t.Fatalf("pending transaction did not see its reservation: %d, %v", available, err)
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SIGTERM did not exit cleanly: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not exit on SIGTERM")
	}

	restart := daemonCommand(t.Context(), t, dir, "-listen", addr, "-data", dir)
	reopenedAddr, _, _ := launchCommand(t, restart)
	if reopenedAddr != addr {
		t.Fatalf("reopened address %s, want %s", reopenedAddr, addr)
	}
	recovered := client.Open(reopenedAddr)
	defer recovered.Close()
	assertOrderRecovered(t, recovered)
	assertBatchOrderAbsent(t, recovered, batchOrder{id: id, sku: sku, request: request})
	if err := reserveCheckpointOrder(t, recovered, id, request, sku).Commit(); err != nil {
		t.Fatalf("retry of the open transaction: %v", err)
	}
	assertCheckpointOrder(t, recovered, id, request, "2,3", sku)
}
