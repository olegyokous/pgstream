package wal

import "testing"

func pickMsg() *Message {
	return &Message{
		Table:  "orders",
		Action: "INSERT",
		Columns: []Column{
			{Name: "id", Value: int64(42)},
			{Name: "amount", Value: "99.99"},
		},
	}
}

func TestNewPicker_EmptyColumnErrors(t *testing.T) {
	_, err := NewPicker("")
	if err == nil {
		t.Fatal("expected error for empty column")
	}
}

func TestNewPicker_ValidColumn(t *testing.T) {
	p, err := NewPicker("id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil picker")
	}
}

func TestPicker_NilMessagePassthrough(t *testing.T) {
	p, _ := NewPicker("id")
	if got := p.Apply(nil); got != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestPicker_ExtractsColumnIntoMeta(t *testing.T) {
	p, _ := NewPicker("id")
	msg := pickMsg()
	out := p.Apply(msg)
	if out.Meta["id"] != "42" {
		t.Fatalf("expected meta[id]=42, got %q", out.Meta["id"])
	}
}

func TestPicker_AbsentColumnLeavesMetaUnchanged(t *testing.T) {
	p, _ := NewPicker("missing")
	msg := pickMsg()
	out := p.Apply(msg)
	if out.Meta != nil && out.Meta["missing"] != "" {
		t.Fatal("expected meta to be untouched for missing column")
	}
}

func TestPicker_CustomMetaKey(t *testing.T) {
	p, _ := NewPicker("id", WithPickerMetaKey("order_id"))
	msg := pickMsg()
	out := p.Apply(msg)
	if out.Meta["order_id"] != "42" {
		t.Fatalf("expected meta[order_id]=42, got %q", out.Meta["order_id"])
	}
}

func TestPicker_TableConstraintSkipsOtherTable(t *testing.T) {
	p, _ := NewPicker("id", WithPickerTable("users"))
	msg := pickMsg() // table=orders
	out := p.Apply(msg)
	if out.Meta != nil && out.Meta["id"] != "" {
		t.Fatal("picker should not apply to non-matching table")
	}
}

func TestPicker_TableConstraintAppliesMatchingTable(t *testing.T) {
	p, _ := NewPicker("amount", WithPickerTable("orders"))
	msg := pickMsg()
	out := p.Apply(msg)
	if out.Meta["amount"] != "99.99" {
		t.Fatalf("expected meta[amount]=99.99, got %q", out.Meta["amount"])
	}
}
