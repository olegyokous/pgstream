package wal

import (
	"context"
	"testing"
	"time"
)

// throttledWriter wraps a writer and throttles each Write call.
type throttledWriter struct {
	th     *Throttler
	inner  []Message
}

func (tw *throttledWriter) Write(ctx context.Context, msg Message) error {
	if err := tw.th.Wait(ctx); err != nil {
		return err
	}
	tw.inner = append(tw.inner, msg)
	return nil
}

func TestThrottler_IntegratesWithMessages(t *testing.T) {
	th := NewThrottler(ThrottleConfig{MessagesPerSecond: 1000, BurstSize: 10})
	tw := &throttledWriter{th: th}

	ctx := context.Background()
	msgs := []Message{
		{Schema: "public", Table: "users", Action: "INSERT"},
		{Schema: "public", Table: "orders", Action: "UPDATE"},
		{Schema: "public", Table: "users", Action: "DELETE"},
	}

	for _, m := range msgs {
		if err := tw.Write(ctx, m); err != nil {
			t.Fatalf("unexpected error writing message: %v", err)
		}
	}

	if len(tw.inner) != len(msgs) {
		t.Errorf("expected %d messages, got %d", len(msgs), len(tw.inner))
	}
}

func TestThrottler_BlocksWhenExhausted(t *testing.T) {
	th := NewThrottler(ThrottleConfig{MessagesPerSecond: 1, BurstSize: 1})
	tw := &throttledWriter{th: th}

	ctx := context.Background()
	// First write uses the burst token.
	_ = tw.Write(ctx, Message{Table: "t", Action: "INSERT"})

	// Second write should block; use a short deadline to verify blocking.
	ctx2, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := tw.Write(ctx2, Message{Table: "t", Action: "INSERT"})
	if err == nil {
		t.Fatal("expected throttle to block and context to expire")
	}
}
