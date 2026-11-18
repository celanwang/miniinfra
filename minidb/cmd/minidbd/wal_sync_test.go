package main

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/client"
)

type batchOrder struct {
	id, sku int
	request string
}

// Run with go test ./cmd/minidbd -run '^TestWALGroupSyncFailureRecoversCompletePrefix$'.
// A test-only daemon injects an I/O error at batch sync and chooses one
// possible torn tail from the normal writer's bytes. An unmodified daemon
// reopens that directory. Hardware durability is outside this fault model.
func TestWALGroupSyncFailureRecoversCompletePrefix(t *testing.T) {
	dir := t.TempDir()
	addr, kill, _ := launchCommand(t, exec.Command(buildSyncFailureDaemon(t), "-listen", "127.0.0.1:0", "-data", dir))
	db := client.Open(addr)
	defer db.Close()
	defer kill()
	setup(t, db)
	execute(t, db, "insert into inventory values (3,10),(4,20),(5,10),(6,20)")

	ack := reserveOrder(t, db)
	orders := []batchOrder{{43, 3, "group-a"}, {44, 5, "group-b"}}
	transactions := make([]*sql.Tx, 0, len(orders))
	for _, order := range orders {
		transactions = append(transactions, reserveCheckpointOrder(t, db, order.id, order.request, order.sku))
	}
	const walFile = "minidb.wal"
	writeDatabaseFile(t, dir, walFile+".armed", nil)
	confirmed := make(chan error, 1)
	go func() { confirmed <- ack.Commit() }()
	waitForSyncGate(t, dir, walFile+".at-confirmed-sync")

	replies := make(chan error, len(transactions))
	for _, tx := range transactions {
		go func() { replies <- tx.Commit() }()
	}
	waitForSyncGate(t, dir, walFile+".at-failed-sync")
	if err := waitForCommitReply(t, confirmed); err != nil {
		t.Fatalf("preceding sync did not confirm its order: %v", err)
	}
	assertOrderRecovered(t, db)
	for _, order := range orders {
		assertBatchOrderAbsent(t, db, order)
	}
	select {
	case err := <-replies:
		t.Fatalf("received a reply before the group's sync returned: %v", err)
	default:
	}
	writeDatabaseFile(t, dir, walFile+".release-failed-sync", nil)
	for range transactions {
		err := waitForCommitReply(t, replies)
		assertUnknownCommit(t, err)
		if !strings.Contains(err.Error(), "sync WAL: "+syscall.EIO.Error()) {
			t.Fatalf("client did not receive the server's sync failure: %v", err)
		}
	}
	var id int
	err := db.QueryRowContext(t.Context(), "select id from orders where id = 42").Scan(&id)
	var dbErr *client.Error
	if !errors.As(err, &dbErr) || dbErr.Code != "unavailable" {
		t.Fatalf("database accepted work after failed sync: %v", err)
	}
	kill()

	data := readDatabaseFile(t, dir, walFile)
	confirmedEnd, err := strconv.Atoi(string(readDatabaseFile(t, dir, walFile+".at-confirmed-sync")))
	if err != nil || confirmedEnd < 0 || confirmedEnd > len(data) {
		t.Fatalf("invalid end of confirmed WAL: %d, %v", confirmedEnd, err)
	}
	prefix, safeEnd := completeBatchRecord(t, data, confirmedEnd)
	kept, lost := orders[0], orders[1]
	if bytes.Contains(prefix, []byte(lost.request)) {
		kept, lost = lost, kept
	}
	if !bytes.Contains(prefix, []byte(kept.request)) || bytes.Contains(prefix, []byte(lost.request)) {
		t.Fatal("could not identify the complete transaction in the surviving WAL")
	}

	addr, _ = launch(t, dir)
	recovered := client.Open(addr)
	defer recovered.Close()
	if got := len(readDatabaseFile(t, dir, walFile)); got != safeEnd {
		t.Fatalf("WAL recovered to %d bytes, want complete prefix of %d", got, safeEnd)
	}
	assertOrderRecovered(t, recovered)
	assertCheckpointOrder(t, recovered, kept.id, kept.request, "2,3", kept.sku)
	assertBatchOrderAbsent(t, recovered, lost)

	// An alternate order ID makes the conflicting key the original request
	// number. The reservation starts with this insert, before any deduction.
	retry := func(order batchOrder) {
		t.Helper()
		tx := begin(t, recovered)
		_, err := tx.ExecContext(t.Context(), "insert into orders values (?,?,'2,3')", order.id+100, order.request)
		var conflict *client.Error
		if !errors.As(err, &conflict) || conflict.Code != client.Unique {
			t.Fatalf("duplicate request %q: %v", order.request, err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
	retry(kept)
	if err := reserveCheckpointOrder(t, recovered, lost.id, lost.request, lost.sku).Commit(); err != nil {
		t.Fatalf("retry of the lost request: %v", err)
	}
	retry(lost)
	assertOrderRecovered(t, recovered)
	for _, order := range orders {
		assertCheckpointOrder(t, recovered, order.id, order.request, "2,3", order.sku)
	}
}

// Inspect both frames emitted by the writer before starting the real recovery.
func completeBatchRecord(t *testing.T, data []byte, start int) ([]byte, int) {
	t.Helper()
	const frameHeader = 12
	batch := data[start:]
	if len(batch) < frameHeader {
		t.Fatal("failed batch has no first frame")
	}
	size := int(binary.BigEndian.Uint32(batch[:4]))
	if size <= 0 || size > len(batch)-frameHeader {
		t.Fatal("failed batch has no complete first frame")
	}
	end := frameHeader + size
	tail := batch[end:]
	if len(tail) < frameHeader || uint64(len(tail)-frameHeader) >= uint64(binary.BigEndian.Uint32(tail[:4])) {
		t.Fatal("failed batch has no torn final payload")
	}
	return batch[frameHeader:end], start + end
}

func assertBatchOrderAbsent(t *testing.T, db *sql.DB, order batchOrder) {
	t.Helper()
	for _, q := range []struct {
		sql string
		arg any
	}{
		{"select id from orders where id = ?", order.id},
		{"select id from orders where request_id = ?", order.request},
		{"select sku from items where order_id = ?", order.id},
	} {
		var id int
		if err := db.QueryRowContext(t.Context(), q.sql, q.arg).Scan(&id); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("%s (%v): %d, %v", q.sql, q.arg, id, err)
		}
	}
	for offset, want := range []int{10, 20} {
		var qty int
		if err := db.QueryRowContext(t.Context(), "select available from inventory where sku = ?", order.sku+offset).Scan(&qty); err != nil || qty != want {
			t.Fatalf("stock for unseen request %q: %d, %v", order.request, qty, err)
		}
	}
}

func waitForSyncGate(t *testing.T, dir, name string) {
	t.Helper()
	path := filepath.Join(dir, name)
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("daemon did not reach %s", name)
}

func waitForCommitReply(t *testing.T, reply <-chan error) error {
	t.Helper()
	select {
	case err := <-reply:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("client did not receive a commit response")
		return nil
	}
}

func buildSyncFailureDaemon(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(root, "internal", "wal", "log.go")
	source, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	const call = "err := log.file.Sync()"
	if bytes.Count(source, []byte(call)) != 1 {
		t.Fatal("expected one WAL batch sync to replace")
	}
	scratch := t.TempDir()
	replaced := filepath.Join(scratch, "log.go")
	writeDatabaseFile(t, scratch, "log.go", bytes.Replace(source, []byte(call), []byte("err := log.syncWithFailure(batch)"), 1))
	overlay, err := json.Marshal(struct{ Replace map[string]string }{map[string]string{
		logPath: replaced,
		filepath.Join(root, "internal", "wal", "group_sync_failure.go"): filepath.Join(root, "cmd", "minidbd", "testdata", "group_sync_failure.go"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	writeDatabaseFile(t, scratch, "overlay.json", overlay)
	daemon := filepath.Join(scratch, "minidbd")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-overlay", filepath.Join(scratch, "overlay.json"), "-o", daemon, "./cmd/minidbd")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build sync-failure daemon: %v\n%s", err, output)
	}
	return daemon
}
