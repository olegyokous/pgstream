package wal

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestTimeouter_IntegratesWithMessages verifies that a Timeouter can wrap
// message-processing logic and propagates results correctly.
func TestTimeouter_IntegratesWithMessages(t *testing.T) {
	msg := Message{
		Table:  "orders",
		Action: "INSERT",
		Columns: []Column{
			{Name: "id", Value: "42"},
		},
	}

	tm := NewTimeouter(TimeoutConfig{Duration: 200 * time.Millisecond})
	var processed Message

	err := tm.Do(context.Background(), func(ctx context.Context) error {
		processed = msg
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed.Table != "orders" {
		t.Fatalf("expected table 'orders', got %q", processed.Table)
	}
}

// TestTimeouter_SlowProcessorIsInterrupted ensures a slow message processor
// is interrupted when the timeout fires.
func TestTimeouter_SlowProcessorIsInterrupted(t *testing.T) {
	tm := NewTimeouter(TimeoutConfig{Duration: 20 * time.Millisecond})

	err := tm.Do(context.Background(), func(ctx context.Context) error {
		// simulate slow downstream sink
		select {
		case <-time.After(1 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}
