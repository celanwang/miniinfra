package main

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
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

type requestFault struct {
	statement     string
	beforeForward bool
	afterSuccess  func()
}

func interruptRequest(t *testing.T, upstream string, fault requestFault) (string, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	dropped := make(chan error, 1)
	go func() {
		defer listener.Close()
		dropped <- proxyUntilFault(listener, upstream, fault)
	}()
	return listener.Addr().String(), dropped
}

func proxyUntilFault(listener net.Listener, upstream string, fault requestFault) error {
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
		target := request.SQL == fault.statement
		if target && fault.beforeForward {
			return nil
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
		if target {
			if response.Code != "" {
				return fmt.Errorf("server did not report success: %s", response.Message)
			}
			if fault.afterSuccess != nil {
				fault.afterSuccess()
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

func TestPoolResetsSessionAfterRejectedCommit(t *testing.T) {
	addr, _ := launch(t, t.TempDir())
	db := client.Open(addr)
	defer db.Close()
	db.SetMaxOpenConns(1)
	execute(t, db, "create table items (id int primary key)")
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	execute(t, conn, "begin")
	execute(t, conn, "insert into items values (1)")
	if _, err := conn.ExecContext(t.Context(), "commit typo"); err == nil {
		t.Fatal("accepted malformed commit")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	execute(t, db, "insert into items values (1)")
}

func TestDriverPreservesUTF8Text(t *testing.T) {
	addr, _ := launch(t, t.TempDir())
	db := client.Open(addr)
	defer db.Close()
	execute(t, db, "create table names (name text primary key)")
	invalid := string([]byte{0xff})
	if _, err := db.ExecContext(t.Context(), "insert into names values (?)", invalid); err == nil {
		t.Error("accepted an invalid UTF-8 parameter")
	}
	if _, err := db.ExecContext(t.Context(), "insert into names values ('x"+invalid+"')"); err == nil {
		t.Error("accepted an invalid UTF-8 SQL literal")
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "{\"SQL\":\"insert into names values ('raw%s')\"}\n", invalid); err != nil {
		t.Fatal(err)
	}
	var response wire.Response
	if err := wire.Read(wire.Reader(conn), &response); err != nil || response.Code == "" {
		t.Fatalf("response to invalid UTF-8 frame: %+v, %v", response, err)
	}
	execute(t, db, "insert into names values (?)", "�")
	execute(t, db, "insert into names values (?)", "x�")
	execute(t, db, "insert into names values (?)", "raw�")
}

func TestTCPHalfCloseDeliversFinalReply(t *testing.T) {
	addr, _ := launch(t, t.TempDir())
	for i := range 12 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", addr, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			frame, err := wire.Encode(wire.Request{SQL: fmt.Sprintf("create table final_%d (id int primary key)", i)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Write(frame); err != nil {
				t.Fatal(err)
			}
			if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
				t.Fatal(err)
			}
			reader := wire.Reader(conn)
			var response wire.Response
			if err := wire.Read(reader, &response); err != nil || response.Code != "" {
				t.Fatalf("final reply: %+v, %v", response, err)
			}
			if err := wire.Read(reader, &response); !errors.Is(err, io.EOF) {
				t.Fatalf("server did not finish the half-closed session: %v", err)
			}
		})
	}
}

func TestTCPPipelineAcrossLargeFrames(t *testing.T) {
	address, _ := launch(t, t.TempDir())
	db := client.Open(address)
	defer db.Close()
	large := strings.Repeat("x", 80<<10)
	execute(t, db, "create table pipeline (id int primary key, note text)")
	execute(t, db, "insert into pipeline values (1,'small'),(2,?)", large)

	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	statements := []string{
		"select id from pipeline where id = 1 and note != '" + large + "'",
		"select note from pipeline where id = 2",
		"select id from pipeline where id = 2",
		"select id from pipeline where id = 1",
		"select id from pipeline where id = 2",
	}
	var pipeline []byte
	for i, statement := range statements {
		frame, err := wire.Encode(wire.Request{SQL: statement})
		if err != nil {
			t.Fatal(err)
		}
		if i == len(statements)-1 {
			frame = frame[:len(frame)-1]
		}
		pipeline = append(pipeline, frame...)
	}
	if _, err := conn.Write(pipeline); err != nil {
		t.Fatal(err)
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	expected := []struct {
		id   int64
		text string
	}{{id: 1}, {text: large}, {id: 2}, {id: 1}, {id: 2}}
	reader := wire.Reader(conn)
	for i, want := range expected {
		var response wire.Response
		if err := wire.Read(reader, &response); err != nil || response.Code != "" ||
			len(response.Result.Rows) != 1 || len(response.Result.Rows[0]) != 1 {
			t.Fatalf("reply %d: code %q, rows %d, error %v", i, response.Code, len(response.Result.Rows), err)
		}
		value := response.Result.Rows[0][0]
		if value.Int != want.id || value.Text != want.text {
			t.Fatalf("unexpected value in reply %d", i)
		}
	}
	var response wire.Response
	if err := wire.Read(reader, &response); !errors.Is(err, io.EOF) {
		t.Fatalf("end of half-closed pipeline: %v", err)
	}
}

func TestOrderSurvivesLostReplyAndAbruptRestart(t *testing.T) {
	dir := t.TempDir()
	addr, kill := launch(t, dir)
	db := client.Open(addr)
	defer db.Close()
	setup(t, db)
	proxy, dropped := interruptRequest(t, addr, requestFault{statement: "COMMIT"})
	connection := client.Open(proxy)
	defer connection.Close()
	tx := reserveOrder(t, connection)
	var id int64
	if err := db.QueryRowContext(t.Context(), "select id from orders where request_id = ?", "r42").Scan(&id); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("second connection saw uncommitted order: %v, %v", id, err)
	}
	assertUnknownCommit(t, tx.Commit())
	awaitInterruption(t, dropped)
	kill()

	addr, _ = launch(t, dir)
	recovered := client.Open(addr)
	defer recovered.Close()
	retry := begin(t, recovered)
	_, err := retry.ExecContext(t.Context(), "insert into orders values (?, ?, ?)", 43, "r42", "1:2,2:3")
	var dbErr *client.Error
	if !errors.As(err, &dbErr) || dbErr.Code != client.Unique {
		t.Fatalf("same request was not rejected: %v", err)
	}
	if err := retry.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertOrderRecovered(t, recovered)
}

func assertUnknownCommit(t *testing.T, err error) {
	t.Helper()
	var dbErr *client.Error
	if !errors.As(err, &dbErr) || dbErr.Code != client.CommitUnknown ||
		errors.Is(err, driver.ErrBadConn) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wanted unknown commit, got %v", err)
	}
}

func awaitInterruption(t *testing.T, interrupted <-chan error) {
	t.Helper()
	select {
	case err := <-interrupted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("proxy did not reach the requested interruption")
	}
}

func reserveOrder(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx := begin(t, db)
	execute(t, tx, "insert into orders values (?, ?, ?)", 42, "r42", "1:2,2:3")
	const deduct = "update inventory set available = available - ? where sku = ? and available >= ?"
	if a, b := execute(t, tx, deduct, 2, 1, 2), execute(t, tx, deduct, 3, 2, 3); a != 1 || b != 1 {
		t.Fatalf("could not reserve both items: %d and %d", a, b)
	}
	execute(t, tx, "insert into items values (42, 1, 2), (42, 2, 3)")
	return tx
}

func assertOrderRecovered(t *testing.T, recovered *sql.DB) {
	t.Helper()
	var id int64
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
