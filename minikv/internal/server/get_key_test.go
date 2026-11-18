package server

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/celanwang/miniinfra/minikv/internal/store"
)

func TestReceiveGETKeyAllocations(t *testing.T) {
	cache, err := store.New(store.Config{Shards: 4, BytesPerShard: 4 << 10, EntriesPerShard: 8, Samples: 8})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(cache, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{16, 32, 33, 64, 128, 256, 1024} {
		hitKey := bytes.Repeat([]byte("h"), size)
		if cache.Set(string(hitKey), []byte("value"), 0, false) != store.Stored {
			t.Fatalf("could not prefill key of length %d", size)
		}
		for _, tc := range []struct {
			name string
			key  []byte
			want store.Result
		}{
			{"hit", hitKey, store.Hit},
			{"miss", bytes.Repeat([]byte("m"), size), store.Miss},
		} {
			t.Run(fmt.Sprintf("%d/%s", size, tc.name), func(t *testing.T) {
				var source bytes.Reader
				reader := bufio.NewReaderSize(&source, headerBytes)
				key := make([]byte, size)
				allocs := testing.AllocsPerRun(1000, func() {
					source.Reset(tc.key)
					reader.Reset(&source)
					value, got, err := s.receive(reader, key, request{keyBytes: size}, 0)
					if err != nil || got != tc.want {
						t.Fatalf("receive GET got %v (%v), want %v", got, err, tc.want)
					}
					if got == store.Hit {
						s.outbound.release(int64(cap(value)))
					}
				})
				if allocs != 0 {
					t.Fatalf("GET key incurred %g allocations/op", allocs)
				}
			})
		}
	}
}

func TestTCPGETBorrowsBinaryKeysAcrossShards(t *testing.T) {
	cache, err := store.New(store.Config{Shards: 4, BytesPerShard: 4 << 10, EntriesPerShard: 4, Samples: 4})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(cache, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	long := "\x00\n\xff" + strings.Repeat("namespace:", 102) + "x"
	short := ""
	for i := range 256 {
		key := string([]byte{'\n', 0, byte(i)})
		if cache.ShardIndex(key) != cache.ShardIndex(long) {
			short = key
			break
		}
	}
	if short == "" {
		t.Fatal("could not find a short key in another shard")
	}
	address, _ := s.listenTestTCP(t)
	c := dialTestTCP(t, address)
	for _, entry := range []struct{ key, value string }{{long, "long"}, {short, "short"}} {
		c.exchange(t, fmt.Sprintf("SET %d %d NONE\n%s%s", len(entry.key), len(entry.value), entry.key, entry.value), "STORED\n")
	}
	c.exchange(t, fmt.Sprintf("SET %d 1 NONE NX\n%sx", len(long), long), "EXISTS\n")
	c.send(t, fmt.Sprintf("GET %d\n%sGET %d\n%sGET %d\n%s", len(short), short, len(long), long, len(long), long[:len(long)-1]+"y"))
	c.value(t, "short")
	c.value(t, "long")
	c.line(t, "MISS\n")
}
