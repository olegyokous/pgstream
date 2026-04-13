package wal

import (
	"testing"
)

func castMsg(table, col string, val any) *Message {
	return &Message{
		Table:  table,
		Action: "INSERT",
		Columns: []Column{
			{Name: col, Value: val},
		},
	}
}

func TestNewCaster_NoRulesErrors(t *testing.T) {
	_, err := NewCaster(nil)
	if err == nil {
		t.Fatal("expected error for empty rules")
	}
}

func TestNewCaster_InvalidTypeErrors(t *testing.T) {
	_, err := NewCaster([]CastRule{{Table: "t", Column: "c", Type: "uuid"}})
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestCaster_NilMessagePassthrough(t *testing.T) {
	c, _ := NewCaster([]CastRule{{Column: "age", Type: "int"}})
	out, err := c.Apply(nil)
	if err != nil || out != nil {
		t.Fatalf("expected nil, nil; got %v, %v", out, err)
	}
}

func TestCaster_CoercesStringToInt(t *testing.T) {
	c, _ := NewCaster([]CastRule{{Table: "users", Column: "age", Type: "int"}})
	msg := castMsg("users", "age", "42")
	out, err := c.Apply(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Columns[0].Value.(int64); !ok || v != 42 {
		t.Fatalf("expected int64(42), got %v (%T)", out.Columns[0].Value, out.Columns[0].Value)
	}
}

func TestCaster_CoercesStringToFloat(t *testing.T) {
	c, _ := NewCaster([]CastRule{{Column: "score", Type: "float"}})
	msg := castMsg("events", "score", "3.14")
	out, err := c.Apply(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Columns[0].Value.(float64); !ok || v < 3.13 {
		t.Fatalf("expected ~3.14, got %v", out.Columns[0].Value)
	}
}

func TestCaster_CoercesStringToBool(t *testing.T) {
	c, _ := NewCaster([]CastRule{{Column: "active", Type: "bool"}})
	msg := castMsg("users", "active", "true")
	out, err := c.Apply(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Columns[0].Value.(bool); !ok || !v {
		t.Fatalf("expected bool(true), got %v", out.Columns[0].Value)
	}
}

func TestCaster_InvalidValueReturnsError(t *testing.T) {
	c, _ := NewCaster([]CastRule{{Column: "age", Type: "int"}})
	msg := castMsg("users", "age", "not-a-number")
	_, err := c.Apply(msg)
	if err == nil {
		t.Fatal("expected coercion error")
	}
}

func TestCaster_TableMismatchSkipsColumn(t *testing.T) {
	c, _ := NewCaster([]CastRule{{Table: "orders", Column: "qty", Type: "int"}})
	msg := castMsg("users", "qty", "5")
	out, err := c.Apply(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := out.Columns[0].Value.(string); !ok || v != "5" {
		t.Fatalf("expected original string value, got %v", out.Columns[0].Value)
	}
}
