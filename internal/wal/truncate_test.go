package wal

import (
	"strings"
	"testing"
)

func truncMsg(valueLen int) *Message {
	return &Message{
		Schema: "public",
		Table:  "events",
		Action: "INSERT",
		Columns: []Column{
			{Name: "payload", Value: strings.Repeat("x", valueLen)},
		},
	}
}

func TestNewTruncator_InvalidMaxBytes(t *testing.T) {
	_, err := NewTruncator(0, false)
	if err == nil {
		t.Fatal("expected error for maxBytes=0")
	}
}

func TestTruncator_SmallMessagePassesThrough(t *testing.T) {
	tr, _ := NewTruncator(DefaultTruncatorMaxBytes, false)
	msg := truncMsg(100)
	out, ok := tr.Apply(msg)
	if !ok {
		t.Fatal("expected message to pass through")
	}
	if out != msg {
		t.Fatal("expected same message pointer")
	}
}

func TestTruncator_OversizedMessageDropped(t *testing.T) {
	tr, _ := NewTruncator(10, false)
	msg := truncMsg(100)
	out, ok := tr.Apply(msg)
	if ok {
		t.Fatal("expected message to be dropped")
	}
	if out != nil {
		t.Fatal("expected nil output when dropped")
	}
}

func TestTruncator_OversizedMessageStubbed(t *testing.T) {
	tr, _ := NewTruncator(10, true)
	msg := truncMsg(100)
	out, ok := tr.Apply(msg)
	if !ok {
		t.Fatal("expected stubbed message to be forwarded")
	}
	if len(out.Columns) != 1 || out.Columns[0].Name != "_truncated" {
		t.Fatalf("expected stub column, got %+v", out.Columns)
	}
	if out.Table != msg.Table {
		t.Fatalf("expected table to be preserved, got %q", out.Table)
	}
}

func TestTruncator_NilMessagePassesThrough(t *testing.T) {
	tr, _ := NewTruncator(DefaultTruncatorMaxBytes, false)
	out, ok := tr.Apply(nil)
	if !ok || out != nil {
		t.Fatal("expected nil message to pass through with ok=true")
	}
}

func TestTruncator_StubbedMessageDoesNotMutateOriginal(t *testing.T) {
	tr, _ := NewTruncator(10, true)
	msg := truncMsg(200)
	origCols := len(msg.Columns)
	tr.Apply(msg)
	if len(msg.Columns) != origCols {
		t.Fatal("Apply must not mutate the original message")
	}
}
