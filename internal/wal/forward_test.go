package wal

import (
	"errors"
	"testing"
)

func forwardMsg(table, action string) *Message {
	return &Message{Relation: table, Action: action}
}

func TestNewForwarder_NilPredicateErrors(t *testing.T) {
	_, err := NewForwarder(nil)
	if err == nil {
		t.Fatal("expected error for nil predicate")
	}
}

func TestForwarder_NilMessagePassthrough(t *testing.T) {
	f, _ := NewForwarder(func(*Message) bool { return true })
	out, err := f.Apply(nil)
	if err != nil || out != nil {
		t.Fatalf("expected (nil, nil), got (%v, %v)", out, err)
	}
}

func TestForwarder_MatchCallsTarget(t *testing.T) {
	var received *Message
	f, _ := NewForwarder(
		func(m *Message) bool { return m.Relation == "orders" },
		WithForwardTarget(func(m *Message) error { received = m; return nil }),
	)
	msg := forwardMsg("orders", "INSERT")
	out, err := f.Apply(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != msg {
		t.Fatal("expected original message returned")
	}
	if received != msg {
		t.Fatal("target was not called with the message")
	}
}

func TestForwarder_NonMatchSkipsTarget(t *testing.T) {
	called := false
	f, _ := NewForwarder(
		func(m *Message) bool { return m.Relation == "orders" },
		WithForwardTarget(func(*Message) error { called = true; return nil }),
	)
	out, err := f.Apply(forwardMsg("users", "INSERT"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatal("expected message returned")
	}
	if called {
		t.Fatal("target should not have been called")
	}
}

func TestForwarder_TargetErrorPropagates(t *testing.T) {
	sentinel := errors.New("write failed")
	f, _ := NewForwarder(
		func(*Message) bool { return true },
		WithForwardTarget(func(*Message) error { return sentinel }),
	)
	_, err := f.Apply(forwardMsg("t", "INSERT"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}

func TestForwarder_DefaultTargetIsNoop(t *testing.T) {
	f, _ := NewForwarder(func(*Message) bool { return true })
	out, err := f.Apply(forwardMsg("t", "UPDATE"))
	if err != nil || out == nil {
		t.Fatalf("unexpected result: out=%v err=%v", out, err)
	}
}
