package wal

import (
	"sync"
	"testing"
)

func TestEvolver_ConcurrentApplyIsSafe(t *testing.T) {
	var mu sync.Mutex
	versions := []int{}
	e, _ := NewEvolver(func(_ string, v int, _ *Message) error {
		mu.Lock()
		versions = append(versions, v)
		mu.Unlock()
		return nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Apply(evolveMsg("users")) //nolint
		}()
	}
	wg.Wait()
	// at least one callback fired (first observe)
	if len(versions) == 0 {
		t.Fatal("expected at least one callback")
	}
}

func TestEvolver_MultipleTablesFireIndependently(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	e, _ := NewEvolver(func(table string, _ int, _ *Message) error {
		mu.Lock()
		calls[table]++
		mu.Unlock()
		return nil
	})

	e.Apply(evolveMsg("users"))  //nolint
	e.Apply(evolveMsg("orders")) //nolint
	e.Apply(evolveMsg("users"))  //nolint
	e.Apply(evolveMsg("orders")) //nolint

	if calls["users"] != 1 || calls["orders"] != 1 {
		t.Fatalf("expected 1 callback each, got users=%d orders=%d", calls["users"], calls["orders"])
	}
}
