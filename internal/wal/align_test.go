package wal

import (
	"testing"
)

func alignMsg(table string, lsn uint64) *Message {
	return &Message{Table: table, LSN: lsn}
}

func TestAligner_NilMessagePassthrough(t *testing.T) {
	a := NewAligner()
	out, err := a.Align(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Fatalf("expected nil, got %+v", out)
	}
}

func TestAligner_FirstMessageAlwaysPasses(t *testing.T) {
	a := NewAligner()
	msg := alignMsg("orders", 100)
	out, err := a.Align(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatal("expected message, got nil")
	}
}

func TestAligner_InOrderMessagePasses(t *testing.T) {
	a := NewAligner()
	a.Align(alignMsg("orders", 100)) //nolint
	out, err := a.Align(alignMsg("orders", 200))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatal("expected message to pass through")
	}
}

func TestAligner_OutOfOrderMessageDropped(t *testing.T) {
	a := NewAligner()
	a.Align(alignMsg("orders", 200)) //nolint
	out, err := a.Align(alignMsg("orders", 100))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Fatalf("expected nil (dropped), got %+v", out)
	}
}

func TestAligner_SameLSNPasses(t *testing.T) {
	a := NewAligner()
	a.Align(alignMsg("orders", 100)) //nolint
	out, err := a.Align(alignMsg("orders", 100))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatal("expected same-LSN message to pass")
	}
}

func TestAligner_DifferentTablesAreIndependent(t *testing.T) {
	a := NewAligner()
	a.Align(alignMsg("orders", 500)) //nolint
	out, err := a.Align(alignMsg("users", 10))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatal("different table should be independent")
	}
}

func TestAligner_ResetClearsState(t *testing.T) {
	a := NewAligner()
	a.Align(alignMsg("orders", 300)) //nolint
	if err := a.Reset("orders"); err != nil {
		t.Fatalf("unexpected error on reset: %v", err)
	}
	_, ok := a.LastLSN("orders")
	if ok {
		t.Fatal("expected state to be cleared after reset")
	}
}

func TestAligner_ResetUnknownTableErrors(t *testing.T) {
	a := NewAligner()
	if err := a.Reset("nonexistent"); err == nil {
		t.Fatal("expected error for unknown table, got nil")
	}
}

func TestAligner_LastLSNTracked(t *testing.T) {
	a := NewAligner()
	a.Align(alignMsg("orders", 42)) //nolint
	v, ok := a.LastLSN("orders")
	if !ok {
		t.Fatal("expected LastLSN to be present")
	}
	if v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}
}
