package main

import (
	"bytes"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/client"
	"github.com/celanwang/miniinfra/minidb/internal/pages"
)

func TestConcurrentOrdersRecoverAcrossCheckpoint(t *testing.T) {
	dir := t.TempDir()
	addr, kill := launch(t, dir)
	db := client.Open(addr)
	defer db.Close()
	setup(t, db)
	execute(t, db, "insert into inventory values (3,10),(4,20),(5,10),(6,20)")
	execute(t, db, "checkpoint")

	acknowledged := reserveOrder(t, db)
	pending := reserveCheckpointOrder(t, db, 43, "pending", 3)
	defer pending.Rollback()
	durable, drop := make(chan struct{}), make(chan struct{})
	proxy, interrupted := interruptRequest(t, addr, requestFault{
		statement: "COMMIT",
		afterSuccess: func() {
			close(durable)
			select {
			case <-drop:
			case <-t.Context().Done():
			}
		},
	})
	connection := client.Open(proxy)
	defer connection.Close()
	unknown := reserveCheckpointOrder(t, connection, 44, "unknown", 5)
	unknownReply := make(chan error, 1)
	go func() { unknownReply <- unknown.Commit() }()
	select {
	case <-durable:
	case err := <-interrupted:
		t.Fatalf("proxy ended before commit became durable: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not reach the proxy")
	}

	// The unknown order is already published, but its caller is still
	// waiting. The other two orders are private across the checkpoint.
	execute(t, db, "checkpoint")
	if err := acknowledged.Commit(); err != nil {
		t.Fatal(err)
	}
	close(drop)
	select {
	case err := <-unknownReply:
		assertUnknownCommit(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("client did not report the lost commit response")
	}
	awaitInterruption(t, interrupted)
	kill()

	addr, _ = launch(t, dir)
	recovered := client.Open(addr)
	defer recovered.Close()
	assertCheckpointOrder(t, recovered, 42, "r42", "1:2,2:3", 1)
	assertCheckpointOrder(t, recovered, 44, "unknown", "2,3", 5)
	for _, query := range []string{
		"select id from orders where request_id = 'pending'",
		"select id from orders where id = 43",
		"select sku from items where order_id = 43",
	} {
		var id int
		if err := recovered.QueryRowContext(t.Context(), query).Scan(&id); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("%s found uncommitted data: %d, %v", query, id, err)
		}
	}
	for sku, want := range map[int]int{3: 10, 4: 20} {
		var amount int
		err := recovered.QueryRowContext(t.Context(), "select available from inventory where sku = ?", sku).Scan(&amount)
		if err != nil || amount != want {
			t.Fatalf("uncommitted inventory %d: %d, %v", sku, amount, err)
		}
	}
	retry := begin(t, recovered)
	_, err := retry.ExecContext(t.Context(), "insert into orders values (45,'unknown','2,3')")
	var dbErr *client.Error
	if !errors.As(err, &dbErr) || dbErr.Code != client.Unique {
		t.Fatalf("retry did not find the durable request key: %v", err)
	}
	if err := retry.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func reserveCheckpointOrder(t *testing.T, pool *sql.DB, id int, request string, firstSKU int) *sql.Tx {
	t.Helper()
	tx := begin(t, pool)
	execute(t, tx, "insert into orders values (?,?,'2,3')", id, request)
	if execute(t, tx, reserveStock, 2, firstSKU, 2) != 1 || execute(t, tx, reserveStock, 3, firstSKU+1, 3) != 1 {
		t.Fatal("could not reserve stock")
	}
	execute(t, tx, "insert into items values (?,?,2),(?,?,3)", id, firstSKU, id, firstSKU+1)
	return tx
}

func assertCheckpointOrder(t *testing.T, db *sql.DB, orderID int, request, digest string, firstSKU int) {
	t.Helper()
	var id int
	var storedDigest string
	if err := db.QueryRowContext(t.Context(), "select id, digest from orders where request_id = ?", request).Scan(&id, &storedDigest); err != nil || id != orderID || storedDigest != digest {
		t.Fatalf("lookup by request %q: %d, %q, %v", request, id, storedDigest, err)
	}
	rows, err := db.QueryContext(t.Context(), "select i.sku, i.qty, v.available from orders o join items i on o.id = i.order_id join inventory v on i.sku = v.sku where o.id = ?", orderID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := map[int][2]int{firstSKU: {2, 8}, firstSKU + 1: {3, 17}}
	for rows.Next() {
		var sku, qty, available int
		if err := rows.Scan(&sku, &qty, &available); err != nil {
			t.Fatal(err)
		}
		if expected, ok := want[sku]; !ok || expected != [2]int{qty, available} {
			t.Fatalf("inconsistent order detail and stock: %d, %d, %d", sku, qty, available)
		}
		delete(want, sku)
	}
	if rows.Err() != nil || len(want) != 0 {
		t.Fatalf("incomplete order: missing=%v, err=%v", want, rows.Err())
	}
}

func TestOrderSurvivesCheckpointPowerLoss(t *testing.T) {
	dir := t.TempDir()
	addr, kill := launch(t, dir)
	db := client.Open(addr)
	t.Cleanup(func() { db.Close() })

	setup(t, db)
	execute(t, db, "checkpoint")
	oldPages := readDatabaseFile(t, dir, "minidb.pages")
	tx := reserveOrder(t, db)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	oldWAL := readDatabaseFile(t, dir, "minidb.wal")

	execute(t, db, "checkpoint")
	newPages := readDatabaseFile(t, dir, "minidb.pages")
	if len(newPages) < 2*pages.Size || bytes.Equal(newPages, oldPages) {
		t.Fatal("second checkpoint did not include the committed order")
	}
	if size := len(readDatabaseFile(t, dir, "minidb.wal")); size >= len(oldWAL) {
		t.Fatal("second checkpoint did not recycle the WAL containing the order")
	}
	kill()

	// Recreate the durable directory at each power-loss point using files
	// emitted by the real daemon. The writer fills the header after the
	// data pages, so the interrupted staging file starts with a zero page.
	torn := make([]byte, pages.Size+pages.Size/2)
	copy(torn[pages.Size:], newPages[pages.Size:len(torn)])
	for _, crash := range []struct {
		name               string
		published, staging []byte
	}{
		{name: "half_page_before_publish", published: oldPages, staging: torn},
		{name: "published_before_WAL_recycle", published: newPages},
	} {
		t.Run(crash.name, func(t *testing.T) {
			image := t.TempDir()
			writeDatabaseFile(t, image, "minidb.pages", crash.published)
			writeDatabaseFile(t, image, "minidb.wal", oldWAL)
			if crash.staging != nil {
				writeDatabaseFile(t, image, "minidb.pages.next", crash.staging)
			}
			addr, _ := launch(t, image)
			db := client.Open(addr)
			defer db.Close()
			assertOrderRecovered(t, db)
			execute(t, db, "checkpoint")
			assertOrderRecovered(t, db)
			if _, err := os.Stat(filepath.Join(image, "minidb.pages.next")); !os.IsNotExist(err) {
				t.Fatalf("checkpoint left a staged file: %v", err)
			}
		})
	}
}

func readDatabaseFile(t *testing.T, dir, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeDatabaseFile(t *testing.T, dir, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
		t.Fatal(err)
	}
}
