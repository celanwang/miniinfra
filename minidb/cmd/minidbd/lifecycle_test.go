package main

import (
	"context"
	"flag"
	"os"
	"os/exec"
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
