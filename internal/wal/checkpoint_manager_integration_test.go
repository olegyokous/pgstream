package wal

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCheckpointManager_ConcurrentTracksAreMonotonic(t *testing.T) {
	var maxFlushed uint64
	var mu sync.Mutex

	cm := NewCheckpointManager(CheckpointConfig{
		FlushInterval: 5 * time.Millisecond,
		FlushFn: func(_ context.Context, lsn uint64) error {
			mu.Lock()
			if lsn > maxFlushed {
				maxFlushed = lsn
			}
			mu.Unlock()
			return nil
		},
	})

	var wg sync.WaitGroup
	for i := uint64(1); i <= 100; i++ {
		wg.Add(1)
		go func(lsn uint64) {
			defer wg.Done()
			cm.Track(lsn)
		}(i)
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_ = cm.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if maxFlushed != 100 {
		t.Fatalf("expected max flushed LSN 100, got %d", maxFlushed)
	}
}

func TestCheckpointManager_CommittedReflectsFlush(t *testing.T) {
	var flushed uint64
	cm := NewCheckpointManager(CheckpointConfig{
		FlushInterval: 5 * time.Millisecond,
		FlushFn: func(_ context.Context, lsn uint64) error {
			atomic.StoreUint64(&flushed, lsn)
			return nil
		},
	})
	cm.Track(77)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_ = cm.Run(ctx)

	if cm.Committed() != 77 {
		t.Fatalf("expected committed LSN 77, got %d", cm.Committed())
	}
}
