package wal

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSemaphore_LimitsConcurrency(t *testing.T) {
	const limit = 3
	const goroutines = 20

	sem, err := NewSemaphore(limit)
	if err != nil {
		t.Fatalf("NewSemaphore: %v", err)
	}

	var active int32
	var maxSeen int32
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sem.Acquire(context.Background()); err != nil {
				t.Errorf("Acquire: %v", err)
				return
			}
			defer sem.Release()

			current := atomic.AddInt32(&active, 1)
			for {
				seen := atomic.LoadInt32(&maxSeen)
				if current <= seen {
					break
				}
				if atomic.CompareAndSwapInt32(&maxSeen, seen, current) {
					break
				}
			}
			atomic.AddInt32(&active, -1)
		}()
	}

	wg.Wait()

	if maxSeen > int32(limit) {
		t.Errorf("concurrency exceeded limit: max active=%d, limit=%d", maxSeen, limit)
	}
}

func TestSemaphore_FullReleaseRestoresCapacity(t *testing.T) {
	sem, _ := NewSemaphore(5)

	for i := 0; i < 5; i++ {
		_ = sem.TryAcquire()
	}
	if sem.Available() != 0 {
		t.Fatalf("expected 0 available, got %d", sem.Available())
	}
	for i := 0; i < 5; i++ {
		sem.Release()
	}
	if sem.Available() != 5 {
		t.Errorf("expected full capacity restored, got %d", sem.Available())
	}
}
