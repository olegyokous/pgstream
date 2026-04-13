package wal

import (
	"testing"
)

func TestCaster_IntegratesWithTransformer(t *testing.T) {
	caster, err := NewCaster([]CastRule{
		{Table: "orders", Column: "amount", Type: "float"},
		{Table: "orders", Column: "qty", Type: "int"},
	})
	if err != nil {
		t.Fatalf("NewCaster: %v", err)
	}

	msg := &Message{
		Table:  "orders",
		Action: "INSERT",
		Columns: []Column{
			{Name: "amount", Value: "99.95"},
			{Name: "qty", Value: "3"},
			{Name: "note", Value: "urgent"},
		},
	}

	out, err := caster.Apply(msg)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if v, ok := out.Columns[0].Value.(float64); !ok || v != 99.95 {
		t.Errorf("amount: expected float64(99.95), got %v (%T)", out.Columns[0].Value, out.Columns[0].Value)
	}
	if v, ok := out.Columns[1].Value.(int64); !ok || v != 3 {
		t.Errorf("qty: expected int64(3), got %v (%T)", out.Columns[1].Value, out.Columns[1].Value)
	}
	if v, ok := out.Columns[2].Value.(string); !ok || v != "urgent" {
		t.Errorf("note: expected string unchanged, got %v", out.Columns[2].Value)
	}
}

func TestCaster_ChainedWithNormalizer(t *testing.T) {
	norm, err := NewNormalizer([]NormalizeRule{
		{Column: "status", Fn: ToLower},
	})
	if err != nil {
		t.Fatalf("NewNormalizer: %v", err)
	}

	caster, err := NewCaster([]CastRule{
		{Column: "score", Type: "float"},
	})
	if err != nil {
		t.Fatalf("NewCaster: %v", err)
	}

	msg := &Message{
		Table:  "events",
		Action: "UPDATE",
		Columns: []Column{
			{Name: "status", Value: "ACTIVE"},
			{Name: "score", Value: "7.5"},
		},
	}

	msg, err = norm.Apply(msg)
	if err != nil {
		t.Fatalf("Normalizer.Apply: %v", err)
	}
	out, err := caster.Apply(msg)
	if err != nil {
		t.Fatalf("Caster.Apply: %v", err)
	}

	if v, ok := out.Columns[0].Value.(string); !ok || v != "active" {
		t.Errorf("status: expected \"active\", got %v", out.Columns[0].Value)
	}
	if v, ok := out.Columns[1].Value.(float64); !ok || v != 7.5 {
		t.Errorf("score: expected float64(7.5), got %v", out.Columns[1].Value)
	}
}
