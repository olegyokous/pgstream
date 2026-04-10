package wal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestCheckpointManager_TrackAdvancesForward(t *testing.T) {
	cm := NewCheckpointManager(DefaultCheckpointConfig(), nil)

	cm.Track(100)
	if got := cm.Latest(); got != 100 {
		t.Fatalf("expected 100, got %d", got)
	}

	// Older LSN must not regress the cursor.
	cm.Track(50)
	if got := cm.Latest(); got != 100 {
		t.Fatalf("expected 100 after regress attempt, got %d", got)
	}

	cm.Track(200)
	if got := cm.Latest(); got != 200 {
		t.Fatalf("expected 200, got %d", got)
	}
}

func TestCheckpointManager_RunFlushesOnInterval(t *testing.T) {
	var flushedLSN atomic.Uint64
	var callCount atomic.Int32

	flushFn := func(ctx context.Context, lsn uint64) error {
		flushedLSN.Store(lsn)
		callCount.Add(1)
		return nil
	}

	cfg := CheckpointConfig{FlushInterval: 50 * time.Millisecond}
	cm := NewCheckpointManager(cfg, flushFn)
	cm.Track(42)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	cm.Run(ctx)

	if flushedLSN.Load() != 42 {
		t.Fatalf("expected flushed LSN 42, got %d", flushedLSN.Load())
	}
	if callCount.Load() == 0 {
		t.Fatal("expected at least one flush call")
	}
}

func TestCheckpointManager_RunSkipsZeroLSN(t *testing.T) {
	var callCount atomic.Int32

	flushFn := func(ctx context.Context, lsn uint64) error {
		callCount.Add(1)
		return nil
	}

	cfg := CheckpointConfig{FlushInterval: 30 * time.Millisecond}
	cm := NewCheckpointManager(cfg, flushFn)
	// Do NOT call Track — latest stays 0.

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	cm.Run(ctx)

	if callCount.Load() != 0 {
		t.Fatalf("expected no flush calls for zero LSN, got %d", callCount.Load())
	}
}

func TestCheckpointManager_RunStopsOnContextCancel(t *testing.T) {
	flushFn := func(ctx context.Context, lsn uint64) error { return nil }

	cfg := CheckpointConfig{FlushInterval: 10 * time.Second}
	cm := NewCheckpointManager(cfg, flushFn)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		cm.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
		// success
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}
