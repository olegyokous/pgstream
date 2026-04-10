package wal

import (
	"testing"
)

// TestRingBuffer_FIFOOrdering verifies messages are returned in insertion order.
func TestRingBuffer_FIFOOrdering(t *testing.T) {
	rb := NewRingBuffer(8)
	tables := []string{"alpha", "beta", "gamma", "delta"}
	for _, tbl := range tables {
		_ = rb.Push(&Message{Table: tbl, Action: "INSERT"})
	}
	for _, want := range tables {
		msg, err := rb.Pop()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg.Table != want {
			t.Fatalf("expected table %q, got %q", want, msg.Table)
		}
	}
}

// TestRingBuffer_DrainAndRefill ensures the buffer can be fully drained and reused.
func TestRingBuffer_DrainAndRefill(t *testing.T) {
	rb := NewRingBuffer(4)
	for i := 0; i < 4; i++ {
		_ = rb.Push(&Message{Action: "INSERT"})
	}
	for i := 0; i < 4; i++ {
		_, _ = rb.Pop()
	}
	if rb.Len() != 0 {
		t.Fatalf("expected empty buffer after drain, got %d", rb.Len())
	}
	for i := 0; i < 4; i++ {
		if err := rb.Push(&Message{Action: "UPDATE"}); err != nil {
			t.Fatalf("refill push %d failed: %v", i, err)
		}
	}
	if rb.Len() != 4 {
		t.Fatalf("expected len 4 after refill, got %d", rb.Len())
	}
}
