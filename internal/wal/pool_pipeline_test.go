package wal

import (
	"context"
	"sync/atomic"
	"testing"
)

// TestPool_IntegratesWithMessages verifies that the pool can process WAL
// messages concurrently in the same way the pipeline would dispatch them.
func TestPool_IntegratesWithMessages(t *testing.T) {
	msgs := []Message{
		{Schema: "public", Table: "orders", Action: "INSERT"},
		{Schema: "public", Table: "orders", Action: "UPDATE"},
		{Schema: "public", Table: "users", Action: "DELETE"},
	}

	p := NewPool(PoolConfig{Workers: 3, QueueDepth: 16})
	var processed int64

	for _, m := range msgs {
		msg := m // capture
		if err := p.Submit(context.Background(), func(ctx context.Context) error {
			if msg.Table == "" {
				t.Errorf("unexpected empty table in message")
			}
			atomic.AddInt64(&processed, 1)
			return nil
		}); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}

	p.Close()

	if processed != int64(len(msgs)) {
		t.Errorf("expected %d processed, got %d", len(msgs), processed)
	}
}
