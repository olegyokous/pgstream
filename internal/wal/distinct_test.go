package wal

import "testing"

func distinctMsg(table, action, col, val string) *Message {
	return &Message{
		Table:  table,
		Action: action,
		Columns: []Column{
			{Name: col, Value: val},
		},
	}
}

func TestNewDistincter_EmptyKeyColErrors(t *testing.T) {
	_, err := NewDistincter("", "")
	if err == nil {
		t.Fatal("expected error for empty keyCol")
	}
}

func TestDistincter_FirstMessagePasses(t *testing.T) {
	d, _ := NewDistincter("id", "")
	msg := distinctMsg("users", "INSERT", "id", "1")
	out, ok := d.Apply(msg)
	if !ok || out == nil {
		t.Fatal("expected first message to pass")
	}
}

func TestDistincter_DuplicateIsDropped(t *testing.T) {
	d, _ := NewDistincter("id", "")
	msg := distinctMsg("users", "INSERT", "id", "42")
	d.Apply(msg)
	_, ok := d.Apply(msg)
	if ok {
		t.Fatal("expected duplicate to be dropped")
	}
}

func TestDistincter_DifferentValuesAreDistinct(t *testing.T) {
	d, _ := NewDistincter("id", "")
	d.Apply(distinctMsg("orders", "INSERT", "id", "1"))
	_, ok := d.Apply(distinctMsg("orders", "INSERT", "id", "2"))
	if !ok {
		t.Fatal("expected different key value to pass")
	}
}

func TestDistincter_TableScopedSkipsOtherTables(t *testing.T) {
	d, _ := NewDistincter("id", "users")
	msg := distinctMsg("orders", "INSERT", "id", "99")
	// First apply – not in scope, should pass.
	_, ok := d.Apply(msg)
	if !ok {
		t.Fatal("out-of-scope table should pass through")
	}
	// Second apply – still not in scope, should still pass.
	_, ok = d.Apply(msg)
	if !ok {
		t.Fatal("out-of-scope table should always pass through")
	}
}

func TestDistincter_NilMessageReturnsFalse(t *testing.T) {
	d, _ := NewDistincter("id", "")
	_, ok := d.Apply(nil)
	if ok {
		t.Fatal("nil message should return false")
	}
}

func TestDistincter_ResetClearsState(t *testing.T) {
	d, _ := NewDistincter("id", "")
	msg := distinctMsg("users", "UPDATE", "id", "7")
	d.Apply(msg)
	if d.Len() != 1 {
		t.Fatalf("expected 1 tracked key, got %d", d.Len())
	}
	d.Reset()
	if d.Len() != 0 {
		t.Fatalf("expected 0 tracked keys after reset, got %d", d.Len())
	}
	// Message should pass again after reset.
	_, ok := d.Apply(msg)
	if !ok {
		t.Fatal("expected message to pass after reset")
	}
}
