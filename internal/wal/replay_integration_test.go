package wal

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReplayBuffer_ConcurrentRecordAndReplay(t *testing.T) {
	rb := NewReplayBuffer(ReplayConfig{MaxMessages: 100, TTL: time.Minute})
	f, err := NewFormatter("json")
	if err != nil {
		t.Fatalf("formatter: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rb.Record(replayMsg("users", "INSERT"))
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		var buf bytes.Buffer
		_, _ = rb.Replay(context.Background(), f, &buf)
	}()

	wg.Wait()
	if rb.Len() > 100 {
		t.Fatalf("buffer exceeded max: %d", rb.Len())
	}
}

func TestReplayBuffer_IntegratesWithPipeline(t *testing.T) {
	rb := NewReplayBuffer(DefaultReplayConfig())
	msgs := []Message{
		{Schema: "public", Table: "events", Action: "INSERT", Columns: map[string]any{"id": 10}},
		{Schema: "public", Table: "events", Action: "UPDATE", Columns: map[string]any{"id": 11}},
		{Schema: "public", Table: "events", Action: "DELETE", Columns: map[string]any{"id": 12}},
	}
	for _, m := range msgs {
		rb.Record(m)
	}

	f, _ := NewFormatter("json")
	var buf bytes.Buffer
	n, err := rb.Replay(context.Background(), f, &buf)
	if err != nil {
		t.Fatalf("replay error: %v", err)
	}
	if n != len(msgs) {
		t.Fatalf("expected %d messages replayed, got %d", len(msgs), n)
	}
	out := buf.String()
	for _, action := range []string{"INSERT", "UPDATE", "DELETE"} {
		if !strings.Contains(out, action) {
			t.Errorf("expected action %q in output", action)
		}
	}
}
