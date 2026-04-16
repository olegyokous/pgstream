package wal

import (
	"testing"
)

func clampMsg(table, col string, val interface{}) *Message {
	return &Message{
		Table:  table,
		Action: "INSERT",
		Columns: []Column{
			{Name: col, Value: val},
		},
	}
}

func TestNewClamper_NoRulesErrors(t *testing.T) {
	_, err := NewClamper(nil)
	if err == nil {
		t.Fatal("expected error for empty rules")
	}
}

func TestNewClamper_InvalidRangeErrors(t *testing.T) {
	_, err := NewClamper([]ClampRule{{Column: "score", Min: 10, Max: 1}})
	if err == nil {
		t.Fatal("expected error for Min > Max")
	}
}

func TestNewClamper_EmptyColumnErrors(t *testing.T) {
	_, err := NewClamper([]ClampRule{{Column: "", Min: 0, Max: 100}})
	if err == nil {
		t.Fatal("expected error for empty column")
	}
}

func TestClamper_NilMessagePassthrough(t *testing.T) {
	c, _ := NewClamper([]ClampRule{{Column: "x", Min: 0, Max: 10}})
	out, err := c.Apply(nil)
	if err != nil || out != nil {
		t.Fatalf("expected nil, nil; got %v, %v", out, err)
	}
}

func TestClamper_ValueBelowMinClamped(t *testing.T) {
	c, _ := NewClamper([]ClampRule{{Column: "age", Min: 0, Max: 120}})
	msg := clampMsg("users", "age", float64(-5))
	out, err := c.Apply(msg)
	if err != nil {
		t.Fatal(err)
	}
	if out.Columns[0].Value.(float64) != 0 {
		t.Fatalf("expected 0, got %v", out.Columns[0].Value)
	}
}

func TestClamper_ValueAboveMaxClamped(t *testing.T) {
	c, _ := NewClamper([]ClampRule{{Column: "score", Min: 0, Max: 100}})
	msg := clampMsg("game", "score", float64(999))
	out, _ := c.Apply(msg)
	if out.Columns[0].Value.(float64) != 100 {
		t.Fatalf("expected 100, got %v", out.Columns[0].Value)
	}
}

func TestClamper_ValueWithinRangeUnchanged(t *testing.T) {
	c, _ := NewClamper([]ClampRule{{Column: "score", Min: 0, Max: 100}})
	msg := clampMsg("game", "score", float64(50))
	out, _ := c.Apply(msg)
	if out.Columns[0].Value.(float64) != 50 {
		t.Fatalf("expected 50, got %v", out.Columns[0].Value)
	}
}

func TestClamper_TableScopedSkipsOtherTable(t *testing.T) {
	c, _ := NewClamper([]ClampRule{{Table: "orders", Column: "qty", Min: 1, Max: 50}})
	msg := clampMsg("products", "qty", float64(200))
	out, _ := c.Apply(msg)
	if out.Columns[0].Value.(float64) != 200 {
		t.Fatalf("expected 200 unchanged, got %v", out.Columns[0].Value)
	}
}

func TestClamper_IntValueClamped(t *testing.T) {
	c, _ := NewClamper([]ClampRule{{Column: "count", Min: 0, Max: 10}})
	msg := clampMsg("stats", "count", int(99))
	out, _ := c.Apply(msg)
	if out.Columns[0].Value.(int) != 10 {
		t.Fatalf("expected 10, got %v", out.Columns[0].Value)
	}
}
