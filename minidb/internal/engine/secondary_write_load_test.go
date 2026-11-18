package engine_test

import (
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/celanwang/miniinfra/minidb/internal/engine"
)

func TestTCPSecondaryIndexWriteLoad(t *testing.T) {
	raw := os.Getenv("MINIDB_SECONDARY_INDEX_ORDERS")
	if raw == "" {
		t.Skip("set MINIDB_SECONDARY_INDEX_ORDERS for the secondary index workload")
	}
	orders, err := strconv.Atoi(raw)
	if err != nil || orders <= 0 {
		t.Fatalf("invalid MINIDB_SECONDARY_INDEX_ORDERS %q", raw)
	}
	for _, mode := range []string{"unique_only", "with_secondary"} {
		t.Run(mode, func(t *testing.T) {
			runSecondaryIndexWrites(t, orders, mode == "with_secondary")
		})
	}
}

func runSecondaryIndexWrites(t *testing.T, orders int, secondary bool) {
	t.Helper()
	store, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	drain := store.ObserveCheckpointLoad()
	_, pool, _ := serveTCP(t, store)
	stockIndex, itemIndex := "", ""
	if secondary {
		stockIndex = ", index by_location (region, bin_label)"
		itemIndex = ", index by_order (order_id)"
	}
	for _, stmt := range []string{
		"create table lock_stock (id int primary key, sku text unique, qty int not null, region text, bin_label text" + stockIndex + ")",
		"create table lock_orders (id int primary key, request_no text unique)",
		"create table lock_items (order_id int, sku_id int, primary key (order_id, sku_id)" + itemIndex + ")",
		"insert into lock_stock values " +
			"(1,'one',0,'north-america-east','assembly-floor-rack-a1')," +
			"(2,'two',0,'north-america-east','assembly-floor-rack-a1')," +
			"(3,'three',0,'north-america-west','assembly-floor-rack-b1')," +
			"(4,'four',0,'north-america-west','assembly-floor-rack-b1')",
	} {
		execSQL(t, pool, stmt)
	}
	drain()

	start := make(chan struct{})
	done := make(chan lockLoadResult, 2)
	for worker := range 2 {
		go func() {
			<-start
			var result lockLoadResult
			for i := range orders {
				sample, err := writeLockOrder(t.Context(), pool, worker*orders+i+1, worker*2+1)
				if err != nil {
					result.err = err
					break
				}
				result.samples = append(result.samples, sample)
			}
			done <- result
		}()
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpuBefore := processCPU(t)
	started := time.Now()
	close(start)
	var samples []lockLoadSample
	for range 2 {
		result := <-done
		if result.err != nil {
			t.Fatal(result.err)
		}
		samples = append(samples, result.samples...)
	}
	wall := time.Since(started)
	cpu := processCPU(t) - cpuBefore
	runtime.ReadMemStats(&after)
	count := float64(len(samples))
	t.Logf("workload orders=%d wall=%s cpu=%s mallocs_per_order=%.2f bytes_per_order=%.2f",
		len(samples), wall, cpu, float64(after.Mallocs-before.Mallocs)/count, float64(after.TotalAlloc-before.TotalAlloc)/count)
	reportLockSQL(t, "secondary_write", samples)
	reportLockWAL(t, drain(), 2*orders)
	for sku := 1; sku <= 4; sku++ {
		if qty := readQty(t, pool, "select qty from lock_stock where id = ?", sku); qty != int64(orders) {
			t.Fatalf("stock %d has quantity %d, want %d", sku, qty, orders)
		}
	}
}
