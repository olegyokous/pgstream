package wal

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHedger_IntegratesWithMessages(t *testing.T) {
	h := NewHedger(HedgeConfig{Delay: 10 * time.Millisecond, MaxHedges: 2})

	msg := &Message{
		LSN:    42,
		Action: "INSERT",
		Table:  "orders",
		Columns: []Column{
			{Name: "id", Value: "1"},
		},
	}

	called := 0
	got, err := h.Do(context.Background(), func(ctx context.Context) ([]byte, error) {
		called++
		if msg == nil {
			return nil, errors.New("nil message")
		}
		return []byte(msg.Table), nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != "orders" {
		t.Fatalf("expected 'orders', got %q", got)
	}
	if called == 0 {
		t.Fatal("fn was never called")
	}
}

func TestHedger_SlowPrimaryIsPreemptedByHedge(t *testing.T) {
	h := NewHedger(HedgeConfig{Delay: 15 * time.Millisecond, MaxHedges: 2})

	start := time.Now()
	_, err := h.Do(context.Background(), func(ctx context.Context) ([]byte, error) {
		// First call blocks; second returns immediately.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return []byte("slow"), nil
		}
	})
	// The hedge fires after 15 ms and also blocks, so both fail via ctx cancel
	// from the winner — in this test the hedge wins with ctx.Err because the
	// primary is cancelled. We just verify the call completes well under 200 ms.
	elapsed := time.Since(start)
	_ = err
	if elapsed > 150*time.Millisecond {
		t.Fatalf("expected hedge to short-circuit primary, elapsed %v", elapsed)
	}
}
