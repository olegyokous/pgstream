package wal

import (
	"strings"
	"testing"
)

func TestSplitter_IntegratesWithMessages(t *testing.T) {
	// Route INSERT/UPDATE to Left, DELETE to Right.
	s, err := NewSplitter(func(m *Message) bool {
		return m.Action == "INSERT" || m.Action == "UPDATE"
	}, DefaultSplitterConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	inputs := []*Message{
		{Table: "users", Action: "INSERT"},
		{Table: "users", Action: "DELETE"},
		{Table: "orders", Action: "UPDATE"},
		{Table: "orders", Action: "DELETE"},
	}
	for _, m := range inputs {
		if err := s.Dispatch(m); err != nil {
			t.Fatalf("dispatch error: %v", err)
		}
	}

	if got := len(s.Left); got != 2 {
		t.Errorf("Left: want 2 messages, got %d", got)
	}
	if got := len(s.Right); got != 2 {
		t.Errorf("Right: want 2 messages, got %d", got)
	}

	for len(s.Left) > 0 {
		m := <-s.Left
		if strings.EqualFold(m.Action, "DELETE") {
			t.Errorf("DELETE should not appear on Left, got %+v", m)
		}
	}
	for len(s.Right) > 0 {
		m := <-s.Right
		if !strings.EqualFold(m.Action, "DELETE") {
			t.Errorf("only DELETE should appear on Right, got %+v", m)
		}
	}
}

func TestSplitter_TablePredicateWithFilter(t *testing.T) {
	// Simulate using the splitter downstream of a filter.
	f := NewFilter(FilterConfig{Tables: []string{"users", "orders"}})

	s, _ := NewSplitter(func(m *Message) bool {
		return m.Table == "users"
	}, DefaultSplitterConfig())

	all := []*Message{
		{Table: "users", Action: "INSERT"},
		{Table: "orders", Action: "INSERT"},
		{Table: "audit", Action: "INSERT"},
	}
	for _, m := range all {
		if f.Match(m) {
			_ = s.Dispatch(m)
		}
	}
	// "users" -> Left, "orders" -> Right, "audit" filtered out
	if got := len(s.Left); got != 1 {
		t.Errorf("Left: want 1, got %d", got)
	}
	if got := len(s.Right); got != 1 {
		t.Errorf("Right: want 1, got %d", got)
	}
}
