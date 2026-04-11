package wal

import (
	"sync"
	"testing"
)

func TestEnveloper_ConcurrentWrapsAreUnique(t *testing.T) {
	env := NewEnveloper(WithEnvelopeSource("integration"))

	const goroutines = 20
	const perGoroutine = 50

	var mu sync.Mutex
	seen := make(map[string]struct{}, goroutines*perGoroutine)
	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				e := env.Wrap("payload", nil)
				mu.Lock()
				if _, dup := seen[e.ID]; dup {
					t.Errorf("duplicate envelope ID: %s", e.ID)
				}
				seen[e.ID] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != goroutines*perGoroutine {
		t.Errorf("expected %d unique IDs, got %d", goroutines*perGoroutine, len(seen))
	}
}

func TestEnveloper_MetaIsIndependent(t *testing.T) {
	env := NewEnveloper()

	meta1 := map[string]any{"table": "orders"}
	meta2 := map[string]any{"table": "users"}

	e1 := env.Wrap("p1", meta1)
	e2 := env.Wrap("p2", meta2)

	if e1.Meta["table"] == e2.Meta["table"] {
		t.Error("envelopes should not share meta")
	}
	if e1.ID == e2.ID {
		t.Error("consecutive envelopes must have distinct IDs")
	}
}
