package wal

import (
	"testing"
)

func gateMsg() *Message {
	return &Message{Table: "orders", Action: "INSERT"}
}

func TestNewGate_StartsOpen(t *testing.T) {
	g := NewGate(true)
	if !g.IsOpen() {
		t.Fatal("expected gate to start open")
	}
}

func TestNewGate_StartsClosed(t *testing.T) {
	g := NewGate(false)
	if g.IsOpen() {
		t.Fatal("expected gate to start closed")
	}
}

func TestGate_OpenPassesMessage(t *testing.T) {
	g := NewGate(true)
	msg := gateMsg()
	if got := g.Apply(msg); got != msg {
		t.Fatalf("expected same message, got %v", got)
	}
}

func TestGate_ClosedDropsMessage(t *testing.T) {
	g := NewGate(false)
	if got := g.Apply(gateMsg()); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestGate_NilMessagePassthrough(t *testing.T) {
	g := NewGate(true)
	if got := g.Apply(nil); got != nil {
		t.Fatalf("expected nil for nil input, got %v", got)
	}
}

func TestGate_OpenAfterClose(t *testing.T) {
	g := NewGate(false)
	g.Open()
	if !g.IsOpen() {
		t.Fatal("expected gate to be open after Open()")
	}
	if got := g.Apply(gateMsg()); got == nil {
		t.Fatal("expected message to pass after Open()")
	}
}

func TestGate_CloseAfterOpen(t *testing.T) {
	g := NewGate(true)
	g.Close()
	if g.IsOpen() {
		t.Fatal("expected gate to be closed after Close()")
	}
	if got := g.Apply(gateMsg()); got != nil {
		t.Fatalf("expected nil after Close(), got %v", got)
	}
}

func TestGate_ToggleMultipleTimes(t *testing.T) {
	g := NewGate(true)
	msg := gateMsg()

	for i := 0; i < 4; i++ {
		if i%2 == 0 {
			g.Close()
			if got := g.Apply(msg); got != nil {
				t.Fatalf("iteration %d: expected nil when closed", i)
			}
		} else {
			g.Open()
			if got := g.Apply(msg); got != msg {
				t.Fatalf("iteration %d: expected message when open", i)
			}
		}
	}
}
