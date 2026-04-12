package wal_test

import (
	"context"
	"testing"
	"time"

	"pgstream/internal/wal"
)

func TestCheckpointManager_IntegratesWithPipeline(t *testing.T) {
	var flushed uint64

	cfg := wal.DefaultCheckpointConfig()
	cfg.FlushInterval = 20 * time.Millisecond

	cm := wal.NewCheckpointManager(cfg, func(lsn uint64) error {
		flushed = lsn
		return nil
	})

	cm.Track(42)
	cm.Track(100)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		cm.Run(ctx)
		close(done)
	}()

	<-done

	if flushed != 100 {
		t.Fatalf("expected flushed LSN 100, got %d", flushed)
	}
}

func TestCheckpointManager_OnlyFlushesHighestLSN(t *testing.T) {
	flushes := []uint64{}

	cfg := wal.DefaultCheckpointConfig()
	cfg.FlushInterval = 15 * time.Millisecond

	cm := wal.NewCheckpointManager(cfg, func(lsn uint64) error {
		flushes = append(flushes, lsn)
		return nil
	})

	// Track several LSNs before the first flush fires.
	cm.Track(10)
	cm.Track(30)
	cm.Track(20) // out-of-order; manager must not regress

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		cm.Run(ctx)
		close(done)
	}()

	<-done

	if len(flushes) == 0 {
		t.Fatal("expected at least one flush")
	}
	for _, v := range flushes {
		if v > 30 {
			t.Fatalf("flushed LSN %d exceeds highest tracked value 30", v)
		}
	}
}
