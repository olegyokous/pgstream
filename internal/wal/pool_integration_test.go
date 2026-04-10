package wal

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestPool_ConcurrentHighLoad(t *testing.T) {
	const taskCount = 500
	p := NewPool(PoolConfig{Workers: 8, QueueDepth: 128})
	var executed int64
	for i := 0; i < taskCount; i++ {
		if err := p.Submit(context.Background(), func(ctx context.Context) error {
			atomic.AddInt64(&executed, 1)
			return nil
		}); err != nil {
			t.Fatalf("submit error: %v", err)
		}
	}
	p.Close()
	if executed != taskCount {
		t.Errorf("expected %d tasks, got %d", taskCount, executed)
	}
}

func TestPool_MultipleCloseIsSafe(t *testing.T) {
	p := NewPool(DefaultPoolConfig())
	// Calling Close multiple times must not panic.
	p.Close()
	p.Close()
	p.Close()
}
