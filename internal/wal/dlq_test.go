package wal

import (
	"context"
	"errors"
	"testing"
	"time"
)

func dlqMsg(action string) *Message {
	return &Message{Action: action, Table: "orders"}
}

func TestDLQ_EnqueueAndLen(t *testing.T) {
	d := NewDeadLetterQueue(DefaultDLQConfig())
	d.Enqueue(dlqMsg("insert"), "decode error")
	d.Enqueue(dlqMsg("update"), "schema mismatch")
	if got := d.Len(); got != 2 {
		t.Fatalf("expected 2 entries, got %d", got)
	}
}

func TestDLQ_DrainClearsQueue(t *testing.T) {
	d := NewDeadLetterQueue(DefaultDLQConfig())
	d.Enqueue(dlqMsg("delete"), "timeout")
	entries := d.Drain()
	if len(entries) != 1 {
		t.Fatalf("expected 1 drained entry, got %d", len(entries))
	}
	if d.Len() != 0 {
		t.Fatal("queue should be empty after Drain")
	}
}

func TestDLQ_EvictsOldestWhenFull(t *testing.T) {
	cfg := DLQConfig{MaxSize: 3, TTL: time.Hour}
	d := NewDeadLetterQueue(cfg)
	for i := 0; i < 4; i++ {
		d.Enqueue(dlqMsg("insert"), "err")
	}
	if d.Len() != 3 {
		t.Fatalf("expected 3 entries after overflow, got %d", d.Len())
	}
}

func TestDLQ_TTLEvictsExpiredEntries(t *testing.T) {
	now := time.Now()
	d := NewDeadLetterQueue(DLQConfig{MaxSize: 100, TTL: time.Minute})
	// inject a clock that starts in the past
	d.clock = func() time.Time { return now.Add(-2 * time.Minute) }
	d.Enqueue(dlqMsg("insert"), "old error")
	// advance clock to present so the entry is expired
	d.clock = func() time.Time { return now }
	if d.Len() != 0 {
		t.Fatal("expired entry should have been evicted")
	}
}

func TestDLQ_ReplayRemovesSuccessfulEntries(t *testing.T) {
	d := NewDeadLetterQueue(DefaultDLQConfig())
	d.Enqueue(dlqMsg("insert"), "err")
	d.Enqueue(dlqMsg("update"), "err")

	called := 0
	_ = d.Replay(context.Background(), func(m *Message) error {
		called++
		return nil // success — should be removed
	})
	if called != 2 {
		t.Fatalf("expected fn called 2 times, got %d", called)
	}
	if d.Len() != 0 {
		t.Fatal("all entries should be removed after successful replay")
	}
}

func TestDLQ_ReplayKeepsFailedEntries(t *testing.T) {
	d := NewDeadLetterQueue(DefaultDLQConfig())
	d.Enqueue(dlqMsg("insert"), "original error")

	replayErr := errors.New("still failing")
	err := d.Replay(context.Background(), func(m *Message) error {
		return replayErr
	})
	if !errors.Is(err, replayErr) {
		t.Fatalf("expected replay error, got %v", err)
	}
	if d.Len() != 1 {
		t.Fatal("failed entry should remain in DLQ")
	}
}

func TestDLQ_ReplayCancelledByContext(t *testing.T) {
	d := NewDeadLetterQueue(DefaultDLQConfig())
	for i := 0; i < 5; i++ {
		d.Enqueue(dlqMsg("insert"), "err")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := d.Replay(ctx, func(m *Message) error { return nil })
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}
