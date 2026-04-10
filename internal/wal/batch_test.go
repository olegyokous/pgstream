package wal

import (
	"context"
	"errors"
	"testing"
	"time"
)

func makeMsgs(n int) []*Message {
	msgs := make([]*Message, n)
	for i := range msgs {
		msgs[i] = &Message{Action: "INSERT", Table: "t"}
	}
	return msgs
}

func TestBatcher_FlushesWhenFull(t *testing.T) {
	var got [][]*Message
	b := NewBatcher(BatchConfig{MaxSize: 3, MaxDelay: time.Minute}, func(batch []*Message) error {
		got = append(got, batch)
		return nil
	})
	for _, m := range makeMsgs(3) {
		if err := b.Add(m); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if len(got) != 1 || len(got[0]) != 3 {
		t.Fatalf("expected 1 batch of 3, got %v batches", len(got))
	}
	if b.Len() != 0 {
		t.Fatalf("buffer should be empty after flush")
	}
}

func TestBatcher_FlushEmpty(t *testing.T) {
	called := false
	b := NewBatcher(DefaultBatchConfig(), func(_ []*Message) error {
		called = true
		return nil
	})
	if err := b.Flush(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Fatal("flush callback should not be called for empty buffer")
	}
}

func TestBatcher_PartialFlush(t *testing.T) {
	var flushed int
	b := NewBatcher(BatchConfig{MaxSize: 10, MaxDelay: time.Minute}, func(batch []*Message) error {
		flushed += len(batch)
		return nil
	})
	for _, m := range makeMsgs(4) {
		_ = b.Add(m)
	}
	if b.Len() != 4 {
		t.Fatalf("expected 4 buffered, got %d", b.Len())
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if flushed != 4 {
		t.Fatalf("expected 4 flushed, got %d", flushed)
	}
}

func TestBatcher_PropagatesFlushError(t *testing.T) {
	sentinel := errors.New("flush error")
	b := NewBatcher(BatchConfig{MaxSize: 1, MaxDelay: time.Minute}, func(_ []*Message) error {
		return sentinel
	})
	err := b.Add(&Message{Action: "INSERT"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}

func TestBatcher_RunFlushesOnTicker(t *testing.T) {
	var count int
	b := NewBatcher(BatchConfig{MaxSize: 100, MaxDelay: 20 * time.Millisecond}, func(batch []*Message) error {
		count += len(batch)
		return nil
	})
	_ = b.Add(&Message{Action: "INSERT"})
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_ = b.Run(ctx)
	if count == 0 {
		t.Fatal("expected at least one flush via ticker")
	}
}
