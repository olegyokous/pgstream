package wal

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func replayMsg(table, action string) Message {
	return Message{Schema: "public", Table: table, Action: action, Columns: map[string]any{"id": 1}}
}

func TestReplayBuffer_RecordAndLen(t *testing.T) {
	rb := NewReplayBuffer(DefaultReplayConfig())
	if rb.Len() != 0 {
		t.Fatalf("expected 0, got %d", rb.Len())
	}
	rb.Record(replayMsg("users", "INSERT"))
	rb.Record(replayMsg("orders", "UPDATE"))
	if rb.Len() != 2 {
		t.Fatalf("expected 2, got %d", rb.Len())
	}
}

func TestReplayBuffer_EvictsOldestWhenFull(t *testing.T) {
	rb := NewReplayBuffer(ReplayConfig{MaxMessages: 3, TTL: time.Minute})
	for i := 0; i < 4; i++ {
		rb.Record(replayMsg("t", "INSERT"))
	}
	if rb.Len() != 3 {
		t.Fatalf("expected 3, got %d", rb.Len())
	}
}

func TestReplayBuffer_Reset(t *testing.T) {
	rb := NewReplayBuffer(DefaultReplayConfig())
	rb.Record(replayMsg("users", "INSERT"))
	rb.Reset()
	if rb.Len() != 0 {
		t.Fatalf("expected 0 after reset, got %d", rb.Len())
	}
}

func TestReplayBuffer_ReplayWritesMessages(t *testing.T) {
	rb := NewReplayBuffer(DefaultReplayConfig())
	rb.Record(replayMsg("users", "INSERT"))
	rb.Record(replayMsg("orders", "DELETE"))

	f, err := NewFormatter("text")
	if err != nil {
		t.Fatalf("formatter: %v", err)
	}
	var buf bytes.Buffer
	n, err := rb.Replay(context.Background(), f, &buf)
	if err != nil {
		t.Fatalf("replay error: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 replayed, got %d", n)
	}
	if !strings.Contains(buf.String(), "users") {
		t.Error("expected 'users' in output")
	}
}

func TestReplayBuffer_SkipsExpiredMessages(t *testing.T) {
	rb := NewReplayBuffer(ReplayConfig{MaxMessages: 10, TTL: time.Millisecond})
	fixed := time.Now()
	rb.clock = func() time.Time { return fixed }
	rb.Record(replayMsg("users", "INSERT"))

	// advance clock past TTL
	rb.clock = func() time.Time { return fixed.Add(time.Second) }

	f, _ := NewFormatter("text")
	var buf bytes.Buffer
	n, err := rb.Replay(context.Background(), f, &buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 replayed (expired), got %d", n)
	}
}

func TestReplayBuffer_ReplayRespectsContextCancellation(t *testing.T) {
	rb := NewReplayBuffer(DefaultReplayConfig())
	for i := 0; i < 5; i++ {
		rb.Record(replayMsg("t", "INSERT"))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	f, _ := NewFormatter("text")
	var buf bytes.Buffer
	_, err := rb.Replay(ctx, f, &buf)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}
