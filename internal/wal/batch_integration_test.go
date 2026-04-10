package wal

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestBatcher_ConcurrentAddAndRun(t *testing.T) {
	var mu sync.Mutex
	var total int

	b := NewBatcher(BatchConfig{MaxSize: 5, MaxDelay: 20 * time.Millisecond}, func(batch []*Message) error {
		mu.Lock()
		total += len(batch)
		mu.Unlock()
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = b.Run(ctx)
	}()

	const n = 20
	for i := 0; i < n; i++ {
		_ = b.Add(&Message{Action: "INSERT", Table: "events"})
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if total < n {
		t.Fatalf("expected at least %d messages flushed, got %d", n, total)
	}
}

func TestBatcher_DrainOnContextCancel(t *testing.T) {
	var flushed int
	b := NewBatcher(BatchConfig{MaxSize: 100, MaxDelay: time.Minute}, func(batch []*Message) error {
		flushed += len(batch)
		return nil
	})
	for _, m := range makeMsgs(7) {
		_ = b.Add(m)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = b.Run(ctx)
	if flushed != 7 {
		t.Fatalf("expected 7 messages flushed on cancel, got %d", flushed)
	}
}
