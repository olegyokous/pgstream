package wal

import (
	"testing"
)

func lookupMsg(table, col, val string) *Message {
	return &Message{
		Table:  table,
		Action: "INSERT",
		Columns: []Column{
			{Name: col, Value: val},
		},
	}
}

func TestNewLookuper_NoRulesErrors(t *testing.T) {
	_, err := NewLookuper(nil)
	if err == nil {
		t.Fatal("expected error for nil rules")
	}
}

func TestNewLookuper_MissingColumnErrors(t *testing.T) {
	_, err := NewLookuper([]LookupRule{
		{Table: "users", Column: "", Map: map[string]string{"a": "b"}},
	})
	if err == nil {
		t.Fatal("expected error for missing column")
	}
}

func TestNewLookuper_EmptyMapErrors(t *testing.T) {
	_, err := NewLookuper([]LookupRule{
		{Table: "users", Column: "status", Map: nil},
	})
	if err == nil {
		t.Fatal("expected error for empty map")
	}
}

func TestLookuper_NilMessagePassthrough(t *testing.T) {
	l, _ := NewLookuper([]LookupRule{
		{Column: "status", Map: map[string]string{"1": "active"}},
	})
	if got := l.Apply(nil); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestLookuper_ReplacesMatchingValue(t *testing.T) {
	l, _ := NewLookuper([]LookupRule{
		{Table: "users", Column: "status", Map: map[string]string{"1": "active", "0": "inactive"}},
	})
	msg := lookupMsg("users", "status", "1")
	out := l.Apply(msg)
	if out.Columns[0].Value != "active" {
		t.Fatalf("expected active, got %v", out.Columns[0].Value)
	}
}

func TestLookuper_UnknownValueKeptWhenNoFallback(t *testing.T) {
	l, _ := NewLookuper([]LookupRule{
		{Table: "users", Column: "status", Map: map[string]string{"1": "active"}},
	})
	msg := lookupMsg("users", "status", "99")
	out := l.Apply(msg)
	if out.Columns[0].Value != "99" {
		t.Fatalf("expected original value 99, got %v", out.Columns[0].Value)
	}
}

func TestLookuper_FallbackAppliedWhenNoMatch(t *testing.T) {
	l, _ := NewLookuper([]LookupRule{
		{Table: "users", Column: "status", Map: map[string]string{"1": "active"}, Fallback: "unknown"},
	})
	msg := lookupMsg("users", "status", "99")
	out := l.Apply(msg)
	if out.Columns[0].Value != "unknown" {
		t.Fatalf("expected unknown, got %v", out.Columns[0].Value)
	}
}

func TestLookuper_TableMismatchSkipsRule(t *testing.T) {
	l, _ := NewLookuper([]LookupRule{
		{Table: "orders", Column: "status", Map: map[string]string{"1": "active"}},
	})
	msg := lookupMsg("users", "status", "1")
	out := l.Apply(msg)
	if out.Columns[0].Value != "1" {
		t.Fatalf("expected original value, got %v", out.Columns[0].Value)
	}
}

func TestLookuper_CaseInsensitiveTableAndColumn(t *testing.T) {
	l, _ := NewLookuper([]LookupRule{
		{Table: "Users", Column: "Status", Map: map[string]string{"0": "inactive"}},
	})
	msg := lookupMsg("users", "status", "0")
	out := l.Apply(msg)
	if out.Columns[0].Value != "inactive" {
		t.Fatalf("expected inactive, got %v", out.Columns[0].Value)
	}
}
