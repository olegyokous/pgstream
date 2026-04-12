package wal

import (
	"testing"
)

func normMsg(table, col string, val interface{}) *Message {
	return &Message{
		Table:  table,
		Action: "INSERT",
		Columns: []Column{
			{Name: col, Value: val},
		},
	}
}

func TestNewNormalizer_NoColumns(t *testing.T) {
	_, err := NewNormalizer(NormalizerConfig{})
	if err == nil {
		t.Fatal("expected error for empty column rules")
	}
}

func TestNormalizer_NilMessagePassthrough(t *testing.T) {
	n, _ := NewNormalizer(NormalizerConfig{Columns: map[string]NormalizeFunc{"x": TrimSpace}})
	if got := n.Apply(nil); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestNormalizer_TrimSpaceApplied(t *testing.T) {
	n, _ := NewNormalizer(NormalizerConfig{
		Columns: map[string]NormalizeFunc{"email": TrimSpace},
	})
	msg := normMsg("users", "email", "  hello@example.com  ")
	got := n.Apply(msg)
	if got.Columns[0].Value != "hello@example.com" {
		t.Fatalf("expected trimmed value, got %q", got.Columns[0].Value)
	}
}

func TestNormalizer_ToLowerApplied(t *testing.T) {
	n, _ := NewNormalizer(NormalizerConfig{
		Columns: map[string]NormalizeFunc{"status": ToLower},
	})
	msg := normMsg("orders", "status", "PENDING")
	got := n.Apply(msg)
	if got.Columns[0].Value != "pending" {
		t.Fatalf("expected lower-case value, got %q", got.Columns[0].Value)
	}
}

func TestNormalizer_TableFilterSkipsOtherTables(t *testing.T) {
	n, _ := NewNormalizer(NormalizerConfig{
		Table:   "users",
		Columns: map[string]NormalizeFunc{"name": ToUpper},
	})
	msg := normMsg("products", "name", "widget")
	got := n.Apply(msg)
	if got.Columns[0].Value != "widget" {
		t.Fatalf("expected unchanged value, got %q", got.Columns[0].Value)
	}
}

func TestNormalizer_TableFilterMatchesTargetTable(t *testing.T) {
	n, _ := NewNormalizer(NormalizerConfig{
		Table:   "users",
		Columns: map[string]NormalizeFunc{"name": ToUpper},
	})
	msg := normMsg("users", "name", "alice")
	got := n.Apply(msg)
	if got.Columns[0].Value != "ALICE" {
		t.Fatalf("expected upper-case value, got %q", got.Columns[0].Value)
	}
}

func TestNormalizer_NonStringValuePassthrough(t *testing.T) {
	n, _ := NewNormalizer(NormalizerConfig{
		Columns: map[string]NormalizeFunc{"count": TrimSpace},
	})
	msg := normMsg("stats", "count", 42)
	got := n.Apply(msg)
	if got.Columns[0].Value != 42 {
		t.Fatalf("expected int passthrough, got %v", got.Columns[0].Value)
	}
}

func TestNormalizer_UnknownColumnUnchanged(t *testing.T) {
	n, _ := NewNormalizer(NormalizerConfig{
		Columns: map[string]NormalizeFunc{"other": ToLower},
	})
	msg := normMsg("users", "name", "Alice")
	got := n.Apply(msg)
	if got.Columns[0].Value != "Alice" {
		t.Fatalf("expected unchanged value, got %q", got.Columns[0].Value)
	}
}
