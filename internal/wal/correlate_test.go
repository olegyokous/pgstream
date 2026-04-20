package wal

import (
	"testing"
)

func correlateMsg(table, col string, val interface{}) *Message {
	return &Message{
		Table:  table,
		Action: "INSERT",
		Columns: []Column{
			{Name: col, Value: val},
			{Name: "other", Value: "x"},
		},
	}
}

func TestNewCorrelater_EmptyColumnErrors(t *testing.T) {
	_, err := NewCorrelater("", "cid")
	if err == nil {
		t.Fatal("expected error for empty column")
	}
}

func TestNewCorrelater_EmptyMetaKeyErrors(t *testing.T) {
	_, err := NewCorrelater("id", "")
	if err == nil {
		t.Fatal("expected error for empty metaKey")
	}
}

func TestNewCorrelater_ValidConfig(t *testing.T) {
	c, err := NewCorrelater("id", "cid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil correlater")
	}
}

func TestCorrelater_NilMessagePassthrough(t *testing.T) {
	c, _ := NewCorrelater("id", "cid")
	out, err := c.Apply(nil)
	if err != nil || out != nil {
		t.Fatalf("expected nil,nil got %v,%v", out, err)
	}
}

func TestCorrelater_StampsMetaFromColumn(t *testing.T) {
	c, _ := NewCorrelater("order_id", "correlation_id")
	msg := correlateMsg("orders", "order_id", "abc-123")
	out, err := c.Apply(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Meta["correlation_id"] != "abc-123" {
		t.Fatalf("expected abc-123, got %v", out.Meta["correlation_id"])
	}
}

func TestCorrelater_MissingColumnLeavesMetaUnset(t *testing.T) {
	c, _ := NewCorrelater("missing", "cid")
	msg := correlateMsg("orders", "order_id", "abc")
	out, err := c.Apply(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Meta != nil && out.Meta["cid"] != nil {
		t.Fatal("meta should not be set for missing column")
	}
}

func TestCorrelater_TableScopeSkipsOtherTables(t *testing.T) {
	c, _ := NewCorrelater("id", "cid", WithCorrelaterTable("orders"))
	msg := correlateMsg("payments", "id", "pay-99")
	out, err := c.Apply(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Meta != nil && out.Meta["cid"] != nil {
		t.Fatal("should not stamp meta for non-matching table")
	}
}

func TestCorrelater_TableScopeMatchesCorrectTable(t *testing.T) {
	c, _ := NewCorrelater("id", "cid", WithCorrelaterTable("orders"))
	msg := correlateMsg("orders", "id", "ord-7")
	out, _ := c.Apply(msg)
	if out.Meta["cid"] != "ord-7" {
		t.Fatalf("expected ord-7 got %v", out.Meta["cid"])
	}
}
