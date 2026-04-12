package wal

import (
	"sync"
	"testing"
)

func TestMessageBuffer_ConcurrentPushPop(t *testing.T) {
	b, err := NewMessageBuffer(BufferConfig{Capacity: 512})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	const producers = 8
	const perProducer = 32

	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				// retry on full
				for {
					if err := b.Push(bufMsg("INSERT", "t")); err == nil {
						break
					}
					// drain a little to make room
					b.Pop()
				}
			}
		}()
	}
	wg.Wait()

	if b.Len() > producers*perProducer {
		t.Errorf("unexpected buffer length %d", b.Len())
	}
}

func TestMessageBuffer_DrainAndRefill(t *testing.T) {
	b, _ := NewMessageBuffer(BufferConfig{Capacity: 8})
	for i := 0; i < 8; i++ {
		if err := b.Push(bufMsg("INSERT", "users")); err != nil {
			t.Fatalf("push %d failed: %v", i, err)
		}
	}
	drained := b.Drain()
	if len(drained) != 8 {
		t.Fatalf("expected 8 drained, got %d", len(drained))
	}
	// refill after drain
	for i := 0; i < 4; i++ {
		if err := b.Push(bufMsg("UPDATE", "orders")); err != nil {
			t.Fatalf("refill push %d failed: %v", i, err)
		}
	}
	if b.Len() != 4 {
		t.Errorf("expected 4 after refill, got %d", b.Len())
	}
}
