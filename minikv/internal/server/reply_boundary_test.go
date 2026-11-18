package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/celanwang/miniinfra/minikv/internal/store"
)

func TestTCPReplyPackingBoundaries(t *testing.T) {
	cache, err := store.New(store.Config{Shards: 1, BytesPerShard: 4 << 10, EntriesPerShard: 4, Samples: 4})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(cache, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	address, _ := s.listenTestTCP(t)
	c := dialTestTCP(t, address)
	small := strings.Repeat("\x00\n\xffs", 128)
	for _, item := range []struct{ key, value string }{{"s", small}, {"l", small + "L"}, {"z", ""}} {
		c.exchange(t, fmt.Sprintf("SET 1 %d NONE\n%s%s", len(item.value), item.key, item.value), "STORED\n")
	}
	c.send(t, "GET 1\nsGET 1\nlGET 1\nzGET 1\nxSET 1 1 NONE NX\ns!GET 1\ns")
	c.value(t, small)
	c.value(t, small+"L")
	c.value(t, "")
	c.line(t, "MISS\n")
	c.line(t, "EXISTS\n")
	c.value(t, small)
	waitBudget(t, &s.outbound, 0)
	waitBudget(t, &s.inbound, 0)
}
