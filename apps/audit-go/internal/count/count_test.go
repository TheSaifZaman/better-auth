package count

import (
	"sync"
	"testing"
)

func TestCounterAdd(t *testing.T) {
	c := New()
	c.Add("w1")
	c.Add("w1")
	c.Add("w2")
	got := c.Snapshot()
	if got["w1"] != 2 || got["w2"] != 1 {
		t.Fatalf("unexpected counts: %+v", got)
	}
}

func TestSnapshotIsCopy(t *testing.T) {
	c := New()
	c.Add("w1")
	snap := c.Snapshot()
	snap["w1"] = 99
	if c.Snapshot()["w1"] != 1 {
		t.Fatal("snapshot must not alias internal state")
	}
}

func TestConcurrentAdd(t *testing.T) {
	c := New()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Add("w1") }()
	}
	wg.Wait()
	if c.Snapshot()["w1"] != 100 {
		t.Fatalf("expected 100, got %d", c.Snapshot()["w1"])
	}
}
