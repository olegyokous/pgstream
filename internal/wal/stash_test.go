package wal

import (
	"fmt"
	"testing"
)

func stashMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestNewStash_DefaultSize(t *testing.T) {
	s, err := NewStash(0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.maxSize != DefaultStashSize {
		t.Errorf("expected maxSize %d, got %d", DefaultStashSize, s.maxSize)
	}
}

func TestStash_PutAndGet(t *testing.T) {
	s, _ := NewStash(8)
	msg := stashMsg("orders", "INSERT")
	if err := s.Put("k1", msg); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok := s.Get("k1")
	if !ok {
		t.Fatal("expected key to be present")
	}
	if got != msg {
		t.Error("returned message does not match stored message")
	}
}

func TestStash_MissingKeyReturnsFalse(t *testing.T) {
	s, _ := NewStash(8)
	_, ok := s.Get("missing")
	if ok {
		t.Error("expected false for missing key")
	}
}

func TestStash_Pop_RemovesEntry(t *testing.T) {
	s, _ := NewStash(8)
	_ = s.Put("k", stashMsg("t", "DELETE"))
	m, ok := s.Pop("k")
	if !ok || m == nil {
		t.Fatal("Pop should return the stored message")
	}
	if s.Len() != 0 {
		t.Errorf("expected len 0 after Pop, got %d", s.Len())
	}
}

func TestStash_CapacityExceeded(t *testing.T) {
	s, _ := NewStash(2)
	_ = s.Put("a", stashMsg("t", "INSERT"))
	_ = s.Put("b", stashMsg("t", "INSERT"))
	if err := s.Put("c", stashMsg("t", "INSERT")); err == nil {
		t.Error("expected capacity error")
	}
}

func TestStash_OverwriteDoesNotGrow(t *testing.T) {
	s, _ := NewStash(2)
	_ = s.Put("a", stashMsg("t", "INSERT"))
	_ = s.Put("b", stashMsg("t", "INSERT"))
	// Overwriting an existing key must not trigger capacity error.
	if err := s.Put("a", stashMsg("t", "UPDATE")); err != nil {
		t.Errorf("unexpected error on overwrite: %v", err)
	}
}

func TestStash_Flush(t *testing.T) {
	s, _ := NewStash(8)
	for i := 0; i < 4; i++ {
		_ = s.Put(fmt.Sprintf("k%d", i), stashMsg("t", "INSERT"))
	}
	out := s.Flush()
	if len(out) != 4 {
		t.Errorf("expected 4 entries in flush snapshot, got %d", len(out))
	}
	if s.Len() != 0 {
		t.Errorf("expected stash empty after flush, got %d", s.Len())
	}
}

func TestStash_EmptyKeyErrors(t *testing.T) {
	s, _ := NewStash(8)
	if err := s.Put("", stashMsg("t", "INSERT")); err == nil {
		t.Error("expected error for empty key")
	}
}
