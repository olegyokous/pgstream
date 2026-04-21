package wal

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestGate_ConcurrentToggleIsSafe(t *testing.T) {
	g := NewGate(true)
	var wg sync.WaitGroup
	const workers = 20

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if (id+j)%2 == 0 {
					g.Open()
				} else {
					g.Close()
				}
				_ = g.Apply(&Message{Table: "t", Action: "INSERT"})
			}
		}(i)
	}
	wg.Wait()
}

func TestGate_OnlyOpenMessagesPassThrough(t *testing.T) {
	g := NewGate(false)
	var passed atomic.Int64

	msgs := make([]*Message, 10)
	for i := range msgs {
		msgs[i] = &Message{Table: "orders", Action: "INSERT"}
	}

	// gate closed: none should pass
	for _, m := range msgs {
		if g.Apply(m) != nil {
			passed.Add(1)
		}
	}
	if passed.Load() != 0 {
		t.Fatalf("expected 0 messages while closed, got %d", passed.Load())
	}

	// gate open: all should pass
	g.Open()
	for _, m := range msgs {
		if g.Apply(m) != nil {
			passed.Add(1)
		}
	}
	if passed.Load() != int64(len(msgs)) {
		t.Fatalf("expected %d messages while open, got %d", len(msgs), passed.Load())
	}
}
