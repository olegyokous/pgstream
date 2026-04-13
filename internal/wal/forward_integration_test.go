package wal

import (
	"sync"
	"testing"
)

func TestForwarder_ConcurrentApplyIsSafe(t *testing.T) {
	var mu sync.Mutex
	collected := make([]*Message, 0)

	f, _ := NewForwarder(
		func(m *Message) bool { return m.Relation == "events" },
		WithForwardTarget(func(m *Message) error {
			mu.Lock()
			collected = append(collected, m)
			mu.Unlock()
			return nil
		}),
	)

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_, _ = f.Apply(forwardMsg("events", "INSERT"))
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(collected) != workers {
		t.Fatalf("expected %d forwarded messages, got %d", workers, len(collected))
	}
}

func TestForwarder_OnlyMatchingTableForwarded(t *testing.T) {
	var mu sync.Mutex
	var forwarded int

	f, _ := NewForwarder(
		func(m *Message) bool { return m.Relation == "audit" },
		WithForwardTarget(func(*Message) error {
			mu.Lock()
			forwarded++
			mu.Unlock()
			return nil
		}),
	)

	tables := []string{"audit", "users", "audit", "orders", "audit"}
	for _, tbl := range tables {
		_, _ = f.Apply(forwardMsg(tbl, "INSERT"))
	}

	mu.Lock()
	defer mu.Unlock()
	if forwarded != 3 {
		t.Fatalf("expected 3 forwarded, got %d", forwarded)
	}
}
