package wal

import (
	"testing"
)

func bufMsg(action, table string) *Message {
	return &Message{Action: action, Table: table}
}

func TestNewMessageBuffer_InvalidCapacity(t *testing.T) {
	_, err := NewMessageBuffer(BufferConfig{Capacity: 0})
	if err == nil {
		t.Fatal("expected error for zero capacity")
	}
}

func TestNewMessageBuffer_ValidCapacity(t *testing.T) {
	b, err := NewMessageBuffer(BufferConfig{Capacity: 4})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Len() != 0 {
		t.Errorf("expected empty buffer, got len=%d", b.Len())
	}
}

func TestMessageBuffer_PushAndPop(t *testing.T) {
	b, _ := NewMessageBuffer(BufferConfig{Capacity: 4})
	msg := bufMsg("INSERT", "users")
	if err := b.Push(msg); err != nil {
		t.Fatalf("push failed: %v", err)
	}
	got := b.Pop()
	if got != msg {
		t.Errorf("expected same message pointer")
	}
}

func TestMessageBuffer_PopEmptyReturnsNil(t *testing.T) {
	b, _ := NewMessageBuffer(BufferConfig{Capacity: 4})
	if got := b.Pop(); got != nil {
		t.Errorf("expected nil from empty buffer, got %v", got)
	}
}

func TestMessageBuffer_FullReturnsError(t *testing.T) {
	b, _ := NewMessageBuffer(BufferConfig{Capacity: 2})
	_ = b.Push(bufMsg("INSERT", "a"))
	_ = b.Push(bufMsg("UPDATE", "b"))
	if err := b.Push(bufMsg("DELETE", "c")); err == nil {
		t.Error("expected error when buffer is full")
	}
}

func TestMessageBuffer_IsFull(t *testing.T) {
	b, _ := NewMessageBuffer(BufferConfig{Capacity: 1})
	if b.IsFull() {
		t.Error("expected not full initially")
	}
	_ = b.Push(bufMsg("INSERT", "t"))
	if !b.IsFull() {
		t.Error("expected full after push")
	}
}

func TestMessageBuffer_Drain(t *testing.T) {
	b, _ := NewMessageBuffer(BufferConfig{Capacity: 4})
	_ = b.Push(bufMsg("INSERT", "a"))
	_ = b.Push(bufMsg("UPDATE", "b"))
	out := b.Drain()
	if len(out) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(out))
	}
	if b.Len() != 0 {
		t.Error("expected buffer empty after drain")
	}
}

func TestMessageBuffer_DefaultConfig(t *testing.T) {
	cfg := DefaultBufferConfig()
	if cfg.Capacity <= 0 {
		t.Errorf("expected positive default capacity, got %d", cfg.Capacity)
	}
}
