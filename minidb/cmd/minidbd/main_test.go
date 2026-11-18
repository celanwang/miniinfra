package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/client"
	"github.com/celanwang/miniinfra/minidb/internal/wire"
)

func TestServeProcess(t *testing.T) {
	dir := os.Getenv("MINIDB_TEST_DAEMON_DIR")
	if dir == "" {
		return
	}
	if err := run(t.Context(), "127.0.0.1:0", dir); err != nil {
		t.Fatal(err)
	}
}

func launch(t *testing.T, dir string) (string, func()) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestServeProcess$")
	cmd.Env = append(os.Environ(), "MINIDB_TEST_DAEMON_DIR="+dir)
	output, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		})
	}
	t.Cleanup(stop)
	address := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			if _, addr, ok := strings.Cut(scanner.Text(), "MiniDB listening on "); ok {
				address <- addr
				return
			}
		}
		address <- ""
	}()
	select {
	case addr := <-address:
		if addr == "" {
			t.Fatal("daemon exited before opening its listener")
		}
		return addr, stop
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not open its listener")
		return "", stop
	}
}

func dropCommitResponse(t *testing.T, upstream string) (string, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	dropped := make(chan error, 1)
	go func() {
		dropped <- proxyUntilCommit(listener, upstream)
	}()
	return listener.Addr().String(), dropped
}

func proxyUntilCommit(listener net.Listener, upstream string) error {
	clientConn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer clientConn.Close()
	dbConn, err := net.DialTimeout("tcp", upstream, 5*time.Second)
	if err != nil {
		return err
	}
	defer dbConn.Close()
	deadline := time.Now().Add(10 * time.Second)
	clientConn.SetDeadline(deadline)
	dbConn.SetDeadline(deadline)
	requestReader, responseReader := wire.Reader(clientConn), wire.Reader(dbConn)
	for {
		var request wire.Request
		if err := wire.Read(requestReader, &request); err != nil {
			return err
		}
		data, err := wire.Encode(request)
		if err != nil {
			return err
		}
		if _, err := dbConn.Write(data); err != nil {
			return err
		}
		var response wire.Response
		if err := wire.Read(responseReader, &response); err != nil {
			return err
		}
		if request.SQL == "COMMIT" {
			if response.Code != "" {
				return fmt.Errorf("server did not commit: %s", response.Message)
			}
			return nil
		}
		data, err = wire.Encode(response)
		if err != nil {
			return err
		}
		if _, err := clientConn.Write(data); err != nil {
			return err
		}
	}
}

type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func execute(t *testing.T, db executor, stmt string, args ...any) int64 {
	t.Helper()
	result, err := db.ExecContext(t.Context(), stmt, args...)
	if err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func setup(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, stmt := range []string{
		"create table inventory (sku int primary key, available int not null)",
		"create table orders (id int primary key, request_id text not null unique, digest text not null)",
		"create table items (order_id int, sku int, qty int, primary key (order_id, sku), index by_order (order_id))",
		"insert into inventory values (1, 10), (2, 20)",
	} {
		execute(t, db, stmt)
	}
}

func begin(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tx.Rollback() })
	return tx
}

func TestOrderSurvivesLostReplyAndAbruptRestart(t *testing.T) {
	dir := t.TempDir()
	addr, kill := launch(t, dir)
	db := client.Open(addr)
	defer db.Close()
	setup(t, db)
	proxy, dropped := dropCommitResponse(t, addr)
	connection := client.Open(proxy)
	defer connection.Close()
	tx := begin(t, connection)
	execute(t, tx, "insert into orders values (?, ?, ?)", 42, "r42", "1:2,2:3")
	const deduct = "update inventory set available = available - ? where sku = ? and available >= ?"
	if a, b := execute(t, tx, deduct, 2, 1, 2), execute(t, tx, deduct, 3, 2, 3); a != 1 || b != 1 {
		t.Fatalf("could not reserve both items: %d and %d", a, b)
	}
	execute(t, tx, "insert into items values (42, 1, 2), (42, 2, 3)")
	var id int64
	if err := db.QueryRowContext(t.Context(), "select id from orders where request_id = ?", "r42").Scan(&id); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("second connection saw uncommitted order: %v, %v", id, err)
	}
	err := tx.Commit()
	var dbErr *client.Error
	if !errors.As(err, &dbErr) || dbErr.Code != client.CommitUnknown {
		t.Fatalf("wanted unknown commit, got %v", err)
	}
	select {
	case err := <-dropped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("proxy did not observe the successful commit")
	}
	kill()

	addr, _ = launch(t, dir)
	recovered := client.Open(addr)
	defer recovered.Close()
	retry := begin(t, recovered)
	_, err = retry.ExecContext(t.Context(), "insert into orders values (?, ?, ?)", 43, "r42", "1:2,2:3")
	if !errors.As(err, &dbErr) || dbErr.Code != client.Unique {
		t.Fatalf("same request was not rejected: %v", err)
	}
	if err := retry.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"select id, digest from orders where id = 42",
		"select id, digest from orders where request_id = 'r42'",
		"select id, digest from orders where id > 41",
	} {
		var digest string
		if err := recovered.QueryRowContext(t.Context(), stmt).Scan(&id, &digest); err != nil || id != 42 || digest != "1:2,2:3" {
			t.Fatalf("%s gave %d %q %v", stmt, id, digest, err)
		}
	}
	result, err := recovered.QueryContext(t.Context(), "select i.sku, i.qty, v.available from orders o join items i on i.order_id = o.id join inventory v on v.sku = i.sku where o.request_id = ?", "r42")
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	want := map[int64][2]int64{1: {2, 8}, 2: {3, 17}}
	for result.Next() {
		var sku, quantity, available int64
		if err := result.Scan(&sku, &quantity, &available); err != nil {
			t.Fatal(err)
		}
		if v, ok := want[sku]; !ok || v != [2]int64{quantity, available} {
			t.Fatalf("unexpected order detail/stock for %d: %d, %d", sku, quantity, available)
		}
		delete(want, sku)
	}
	if result.Err() != nil || len(want) != 0 {
		t.Fatalf("lost details: %v %v", want, result.Err())
	}
}
