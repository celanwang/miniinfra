package server

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/celanwang/miniinfra/minikv/internal/store"
)

func TestReceiveSETKeyAllocations(t *testing.T) {
	for _, keySize := range []int{16, 64, 256} {
		for _, valueSize := range []int{0, 16} {
			for _, tc := range []struct {
				name   string
				insert bool
				nx     bool
				want   store.Result
			}{
				{"overwrite", false, false, store.Stored},
				{"exists", false, true, store.Exists},
				{"insert", true, false, store.Stored},
			} {
				t.Run(fmt.Sprintf("key%d/value%d/%s", keySize, valueSize, tc.name), func(t *testing.T) {
					s := testServer(t, 1, nil)
					key := bytes.Repeat([]byte("k"), keySize)
					if got := s.cache.Set(key, nil, 0, false); got != store.Stored {
						t.Fatalf("prefill returned %v", got)
					}
					frames := [2][]byte{
						append(bytes.Clone(key), bytes.Repeat([]byte("v"), valueSize)...),
						append(bytes.Clone(key), bytes.Repeat([]byte("v"), valueSize)...),
					}
					frames[1][0] = 'j'
					var source bytes.Reader
					reader := bufio.NewReaderSize(&source, headerBytes)
					frameIndex := 0
					req := request{keyBytes: keySize, valueBytes: valueSize, write: true, onlyMiss: tc.nx}
					allocs := testing.AllocsPerRun(1000, func() {
						if tc.insert {
							// The alternate key is absent because there is only one slot.
							frameIndex ^= 1
						}
						source.Reset(frames[frameIndex])
						reader.Reset(&source)
						_, got, err := s.receive(reader, key, req, valueSize)
						if err != nil || got != tc.want {
							t.Fatalf("receive SET got %v (%v), want %v", got, err, tc.want)
						}
					})
					var want float64
					if tc.insert {
						want++
					}
					if valueSize > 0 {
						want++
					}
					if allocs != want {
						t.Fatalf("receive SET incurred %g allocations/op, want %g", allocs, want)
					}
				})
			}
		}
	}
}
