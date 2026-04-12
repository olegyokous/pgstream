package wal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestHedger_ConcurrentHedgesAreSafe(t *testing.T) {
	h := NewHedger(HedgeConfig{Delay: 5 * time.Millisecond, MaxHedges: 3})
	var calls int32

	const goroutines = 20
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			_, err := h.Do(context.Background(), func(ctx context.Context) ([]byte, error) {
				atomic.AddInt32(&calls, 1)
				return []byte("ok"), nil
			})
			errs <- err
		}()
	}

	for i := 0; i < goroutines; i++ {
		if err := <-errs; err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if c := atomic.LoadInt32(&calls); c < goroutines {
		t.Errorf("expected at least %d calls, got %d", goroutines, c)
	}
}

func TestHedger_OnlyOneSuccessIsReturned(t *testing.T) {
	h := NewHedger(HedgeConfig{Delay: 10 * time.Millisecond, MaxHedges: 3})
	var wins int32

	const iters = 50
	for i := 0; i < iters; i++ {
		val, err := h.Do(context.Background(), func(ctx context.Context) ([]byte, error) {
			time.Sleep(2 * time.Millisecond)
			return []byte("v"), nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(val) == "v" {
			atomic.AddInt32(&wins, 1)
		}
	}
	if int(atomic.LoadInt32(&wins)) != iters {
		t.Errorf("expected %d wins, got %d", iters, wins)
	}
}
