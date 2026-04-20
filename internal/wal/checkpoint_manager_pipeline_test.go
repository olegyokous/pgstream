package wal

import (
	"context"
	"testing"
	"time"
)

func TestCheckpointManager_IntegratesWithMessages(t *testing.T) {
	var lastFlushed uint64
	cm := NewCheckpointManager(CheckpointConfig{
		FlushInterval: 5 * time.Millisecond,
		FlushFn: func(_ context.Context, lsn uint64) error {
			lastFlushed = lsn
			return nil
		},
	})

	msgs := []*Message{
		{LSN: 10, Action: "INSERT", Table: "orders"},
		{LSN: 20, Action: "UPDATE", Table: "orders"},
		{LSN: 30, Action: "DELETE", Table: "orders"},
	}

	for _, m := range msgs {
		cm.Track(m.LSN)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_ = cm.Run(ctx)

	if lastFlushed != 30 {
		t.Fatalf("expected last flushed LSN 30, got %d", lastFlushed)
	}
}

func TestCheckpointManager_OnlyFlushesHighestLSN(t *testing.T) {
	var flushCount int
	var lastLSN uint64
	cm := NewCheckpointManager(CheckpointConfig{
		FlushInterval: 5 * time.Millisecond,
		FlushFn: func(_ context.Context, lsn uint64) error {
			flushCount++
			lastLSN = lsn
			return nil
		},
	})

	cm.Track(5)
	cm.Track(5) // duplicate — should not re-flush

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = cm.Run(ctx)

	if flushCount != 1 {
		t.Fatalf("expected exactly 1 flush, got %d", flushCount)
	}
	if lastLSN != 5 {
		t.Fatalf("expected flushed LSN 5, got %d", lastLSN)
	}
}

func TestCheckpointManager_NoFlushWithoutTrackedLSN(t *testing.T) {
	var flushCount int
	cm := NewCheckpointManager(CheckpointConfig{
		FlushInterval: 5 * time.Millisecond,
		FlushFn: func(_ context.Context, _ uint64) error {
			flushCount++
			return nil
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = cm.Run(ctx)

	if flushCount != 0 {
		t.Fatalf("expected 0 flushes with no tracked LSN, got %d", flushCount)
	}
}
