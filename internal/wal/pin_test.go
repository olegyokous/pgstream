package wal

import (
	"testing"
)

func pinMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestNewPinner_DefaultKeyFn(t *testing.T) {
	p, err := NewPinner()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil pinner")
	}
}

func TestNewPinner_NilKeyFnErrors(t *testing.T) {
	_, err := NewPinner(WithPinnerKeyFn(nil))
	if err == nil {
		t.Fatal("expected error for nil keyFn")
	}
}

func TestPinner_NilMessageIsNotPinned(t *testing.T) {
	p, _ := NewPinner()
	if p.IsPinned(nil) {
		t.Fatal("nil message should not be pinned")
	}
}

func TestPinner_PinAndCheck(t *testing.T) {
	p, _ := NewPinner()
	m := pinMsg("orders", "INSERT")
	if p.IsPinned(m) {
		t.Fatal("should not be pinned before Pin()")
	}
	p.Pin("orders")
	if !p.IsPinned(m) {
		t.Fatal("should be pinned after Pin()")
	}
}

func TestPinner_UnpinRemovesKey(t *testing.T) {
	p, _ := NewPinner()
	p.Pin("orders")
	p.Unpin("orders")
	if p.IsPinned(pinMsg("orders", "INSERT")) {
		t.Fatal("should not be pinned after Unpin()")
	}
}

func TestPinner_LenTracksKeys(t *testing.T) {
	p, _ := NewPinner()
	p.Pin("a")
	p.Pin("b")
	if p.Len() != 2 {
		t.Fatalf("expected 2, got %d", p.Len())
	}
	p.Unpin("a")
	if p.Len() != 1 {
		t.Fatalf("expected 1, got %d", p.Len())
	}
}

func TestPinner_CustomKeyFnByAction(t *testing.T) {
	p, _ := NewPinner(WithPinnerKeyFn(func(m *Message) string { return m.Action }))
	p.Pin("DELETE")
	if !p.IsPinned(pinMsg("any", "DELETE")) {
		t.Fatal("DELETE action should be pinned")
	}
	if p.IsPinned(pinMsg("any", "INSERT")) {
		t.Fatal("INSERT action should not be pinned")
	}
}
