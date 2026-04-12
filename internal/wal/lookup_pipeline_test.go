package wal

import (
	"testing"
)

func TestLookuper_IntegratesWithTransformer(t *testing.T) {
	lookuper, err := NewLookuper([]LookupRule{
		{
			Table:  "orders",
			Column: "state",
			Map:    map[string]string{"1": "pending", "2": "shipped", "3": "delivered"},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tr, err := NewTransformer([]TransformFunc{
		func(m *Message) *Message { return lookuper.Apply(m) },
	})
	if err != nil {
		t.Fatalf("unexpected transformer error: %v", err)
	}

	msg := &Message{
		Table:  "orders",
		Action: "UPDATE",
		Columns: []Column{
			{Name: "id", Value: "42"},
			{Name: "state", Value: "2"},
		},
	}

	out := tr.Apply(msg)
	if out == nil {
		t.Fatal("expected message, got nil")
	}

	var stateVal string
	for _, c := range out.Columns {
		if c.Name == "state" {
			stateVal, _ = c.Value.(string)
		}
	}
	if stateVal != "shipped" {
		t.Fatalf("expected shipped, got %q", stateVal)
	}
}

func TestLookuper_MultipleRulesAppliedInOrder(t *testing.T) {
	lookuper, _ := NewLookuper([]LookupRule{
		{
			Column: "role",
			Map:    map[string]string{"1": "admin", "2": "user"},
		},
		{
			Table:    "accounts",
			Column:   "tier",
			Map:      map[string]string{"gold": "premium"},
			Fallback: "standard",
		},
	})

	msg := &Message{
		Table:  "accounts",
		Action: "INSERT",
		Columns: []Column{
			{Name: "role", Value: "1"},
			{Name: "tier", Value: "silver"},
		},
	}

	out := lookuper.Apply(msg)

	vals := map[string]string{}
	for _, c := range out.Columns {
		v, _ := c.Value.(string)
		vals[c.Name] = v
	}

	if vals["role"] != "admin" {
		t.Errorf("expected admin, got %q", vals["role"])
	}
	if vals["tier"] != "standard" {
		t.Errorf("expected standard, got %q", vals["tier"])
	}
}
