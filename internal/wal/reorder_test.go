package wal

import (
	"testing"
)

func reorderMsg(lsn uint64) *Message {
	return &Message{
		Table:  "events",
		Action: "INSERT",
		Meta:   map[string]any{"lsn": lsn},
	}
}

func TestNewReorderer_InvalidBufferSize(t *testing.T) {
	_, err := NewReorderer(ReordererConfig{BufferSize: 0, Field: "lsn"})
	if err == nil {
		t.Fatal("expected error for zero BufferSize")
	}
}

func TestNewReorderer_EmptyField(t *testing.T) {
	_, err := NewReorderer(ReordererConfig{BufferSize: 4, Field: ""})
	if err == nil {
		t.Fatal("expected error for empty Field")
	}
}

func TestNewReorderer_DefaultConfig(t *testing.T) {
	cfg := DefaultReordererConfig()
	r, err := NewReorderer(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Len() != 0 {
		t.Fatalf("expected empty buffer, got %d", r.Len())
	}
}

func TestReorderer_NilMessageIsIgnored(t *testing.T) {
	r, _ := NewReorderer(ReordererConfig{BufferSize: 4, Field: "lsn"})
	out := r.Add(nil)
	if out != nil {
		t.Fatal("expected nil output for nil message")
	}
	if r.Len() != 0 {
		t.Fatalf("expected empty buffer, got %d", r.Len())
	}
}

func TestReorderer_BuffersUntilFull(t *testing.T) {
	r, _ := NewReorderer(ReordererConfig{BufferSize: 3, Field: "lsn"})
	if out := r.Add(reorderMsg(3)); out != nil {
		t.Fatal("expected nil before buffer full")
	}
	if out := r.Add(reorderMsg(1)); out != nil {
		t.Fatal("expected nil before buffer full")
	}
	out := r.Add(reorderMsg(2))
	if len(out) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(out))
	}
}

func TestReorderer_EmitsSortedByField(t *testing.T) {
	r, _ := NewReorderer(ReordererConfig{BufferSize: 4, Field: "lsn"})
	for _, lsn := range []uint64{40, 10, 30, 20} {
		r.Add(reorderMsg(lsn))
	}
	out := r.Flush()
	if len(out) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(out))
	}
	expected := []uint64{10, 20, 30, 40}
	for i, msg := range out {
		got, _ := msg.Meta["lsn"].(uint64)
		if got != expected[i] {
			t.Errorf("pos %d: expected lsn %d, got %d", i, expected[i], got)
		}
	}
}

func TestReorderer_FlushEmptyReturnsNil(t *testing.T) {
	r, _ := NewReorderer(ReordererConfig{BufferSize: 4, Field: "lsn"})
	if out := r.Flush(); out != nil {
		t.Fatalf("expected nil on empty flush, got %v", out)
	}
}

func TestReorderer_FlushClearsBuffer(t *testing.T) {
	r, _ := NewReorderer(ReordererConfig{BufferSize: 4, Field: "lsn"})
	r.Add(reorderMsg(5))
	r.Add(reorderMsg(2))
	r.Flush()
	if r.Len() != 0 {
		t.Fatalf("expected empty buffer after flush, got %d", r.Len())
	}
}
