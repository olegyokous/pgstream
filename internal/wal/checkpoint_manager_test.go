package wal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestCheckpointManager_TrackAdvancesForward(t *testing.T) {
	flushed := uint64(0)
	cm := NewCheckpointManager(DefaultCheckpointManagerConfig(func(_ context.Context, lsn uint64) error {
		atomic.StoreUint64(&flushed, lsn)
		return nil
	}))

	cm.Track(100)
	cm.Track(50) // should not regress
	cm.Track(200)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	cm.cfg.FlushInterval = 5 * time.Millisecond
	_ = cm.Run(ctx)

	if got := atomic.LoadUint64(&flushed); got != 200 {
		t.Fatalf("expected flushed LSN 200, got %d", got)
	}
}

func TestCheckpointManager_RunFlushesOnInterval(t *testing.T) {
	var calls int
	cm := NewCheckpointManager(CheckpointConfig{
		FlushInterval: 5 * time.Millisecond,
		FlushFn: func(_ context.Context, lsn uint64) error {
			calls++
			return nil
		},
	})
	cm.Track(42)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_ = cm.Run(ctx)

	if calls == 0 {
		t.Fatal("expected at least one flush call")
	}
}

func TestCheckpointManager_RunSkipsZeroLSN(t *testing.T) {
	var calls int
	cm := NewCheckpointManager(CheckpointConfig{
		FlushInterval: 5 * time.Millisecond,
		FlushFn: func(_ context.Context, _ uint64) error {
			calls++
			return nil
		},
	})
	// deliberately do not call Track

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = cm.Run(ctx)

	if calls != 0 {
		t.Fatalf("expected zero flush calls with no tracked LSN, got %d", calls)
	}
}

func TestCheckpointManager_RunStopsOnContextCancel(t *testing.T) {
	cm := NewCheckpointManager(CheckpointConfig{
		FlushInterval: 1 * time.Second,
		FlushFn:       func(_ context.Context, _ uint64) error { return nil },
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cm.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not stop after context cancellation")
	}
}
