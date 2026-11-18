package main

import (
	"testing"
	"time"
)

func TestOfferDistinguishesQueuePressureFromLateScheduler(t *testing.T) {
	queue := make(chan arrival, 1)
	at := time.Unix(20, 0)
	var count counts
	next := offer(queue, &count, at, false, 0, 2)
	next = offer(queue, &count, at, true, next, 5)
	if next != 5 || count.Offered != 5 || count.Waiting != 1 || count.Late != 3 {
		t.Fatalf("next = %d, counts = %+v", next, count)
	}
	if got := <-queue; got.sequence != 0 || !got.at.Equal(at) {
		t.Fatalf("unexpected arrival: %+v", got)
	}
}

func TestScheduleCountsPartialLastInterval(t *testing.T) {
	var normal, auxiliary counts
	start := time.Now().Add(-time.Minute)
	schedule(start, 1250*time.Microsecond, 350*time.Microsecond, 100000,
		make(chan arrival, 1), make(chan arrival, 1), &normal, &auxiliary)
	if normal.Offered != 125 || normal.Late != 125 || auxiliary.Offered != 0 {
		t.Fatalf("normal = %+v, auxiliary = %+v", normal, auxiliary)
	}
}
