package wal

import (
	"testing"
)

func flatMsg() *Message {
	return &Message{
		Table:  "orders",
		Action: "INSERT",
		Columns: []Column{
			{Name: "id", Value: "42"},
			{Name: "status", Value: "pending"},
		},
	}
}

func TestFlattener_NilMessageReturnsNil(t *testing.T) {
	f := NewFlattener()
	if got := f.Flatten(nil); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestFlattener_NoPrefix(t *testing.T) {
	f := NewFlattener()
	out := f.Flatten(flatMsg())
	if len(out) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(out))
	}
	if out["id"] != "42" {
		t.Errorf("expected id=42, got %q", out["id"])
	}
	if out["status"] != "pending" {
		t.Errorf("expected status=pending, got %q", out["status"])
	}
}

func TestFlattener_WithPrefix(t *testing.T) {
	f := NewFlattener(WithFlattenPrefix())
	out := f.Flatten(flatMsg())
	if _, ok := out["orders.id"]; !ok {
		t.Errorf("expected key orders.id, got keys %v", out)
	}
	if _, ok := out["orders.status"]; !ok {
		t.Errorf("expected key orders.status, got keys %v", out)
	}
}

func TestFlattener_CustomSeparator(t *testing.T) {
	f := NewFlattener(WithFlattenPrefix(), WithFlattenSeparator("_"))
	out := f.Flatten(flatMsg())
	if _, ok := out["orders_id"]; !ok {
		t.Errorf("expected key orders_id, got keys %v", out)
	}
}

func TestFlattener_NilColumnValueIsEmptyString(t *testing.T) {
	msg := &Message{
		Table:  "users",
		Action: "UPDATE",
		Columns: []Column{
			{Name: "email", Value: nil},
		},
	}
	f := NewFlattener()
	out := f.Flatten(msg)
	if v, ok := out["email"]; !ok || v != "" {
		t.Errorf("expected empty string for nil value, got %q (ok=%v)", v, ok)
	}
}

func TestFlattener_EmptyColumnsReturnsEmptyMap(t *testing.T) {
	msg := &Message{Table: "empty", Action: "DELETE", Columns: []Column{}}
	f := NewFlattener()
	out := f.Flatten(msg)
	if len(out) != 0 {
		t.Errorf("expected empty map, got %v", out)
	}
}
