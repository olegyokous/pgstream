package wal

import (
	"testing"
)

func freezeMsg(table, action string) *Message {
	return &Message{Relation: table, Action: action}
}

func TestNewFreezer_ZeroMaxErrors(t *testing.T) {
	_, err := NewFreezer(0)
	if err == nil {
		t.Fatal("expected error for zero maxBuf")
	}
}

func TestNewFreezer_ValidMax(t *testing.T) {
	f, err := NewFreezer(10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.IsFrozen() {
		t.Fatal("expected not frozen initially")
	}
}

func TestFreezer_NilMessagePassthrough(t *testing.T) {
	f, _ := NewFreezer(5)
	if got := f.Apply(nil); got != nil {
		t.Fatal("expected nil for nil message")
	}
}

func TestFreezer_NotFrozenPassesThrough(t *testing.T) {
	f, _ := NewFreezer(5)
	msg := freezeMsg("orders", "INSERT")
	if got := f.Apply(msg); got != msg {
		t.Fatal("expected message to pass through when not frozen")
	}
}

func TestFreezer_FrozenBuffersMessage(t *testing.T) {
	f, _ := NewFreezer(5)
	f.Freeze()
	msg := freezeMsg("orders", "INSERT")
	if got := f.Apply(msg); got != nil {
		t.Fatal("expected nil when frozen")
	}
	if f.Len() != 1 {
		t.Fatalf("expected 1 buffered, got %d", f.Len())
	}
}

func TestFreezer_ThawReleasesBuffer(t *testing.T) {
	f, _ := NewFreezer(5)
	f.Freeze()
	msgs := []*Message{
		freezeMSg("a", "INSERT"),
		freezeMSg("b", "UPDATE"),
	}
	for _, m := range msgs {
		f.Apply(m)
	}
	out := f.Thaw()
	if len(out) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(out))
	}
	if f.Len() != 0 {
		t.Fatal("expected buffer cleared after thaw")
	}
	if f.IsFrozen() {
		t.Fatal()
	}
}

func TestFreezer_EvictsOldestWhenFull(t *testing.T) {
	f, _ := NewFreezer(2)
	f.Freeze()
	m1 := freezeMsg("t1", "INSERT")
	m2 := freezeMsg("t2", "INSERT")
	m3 := freezeMsg("t3", "INSERT")
	f.Apply(m1)
	f.Apply(m2)
	f.Apply(m3) // should evict m1
	out := f.Thaw()
	if len(out) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(out))
	}
	if out[0] != m2 || out[1] != m3 {
		t.Fatal("expected oldest evicted")
	}
}

func freezeMSg(table, action string) *Message {
	return &Message{Relation: table, Action: action}
}
