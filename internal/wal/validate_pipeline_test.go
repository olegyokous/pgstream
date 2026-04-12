package wal

import (
	"errors"
	"testing"
)

// TestValidator_IntegratesWithFilter verifies that a Validator can be wired
// before a Filter so that structurally invalid messages are rejected before
// any filter predicate runs.
func TestValidator_IntegratesWithFilter(t *testing.T) {
	v, err := NewValidator(RequireTable(), RequireAction("INSERT", "UPDATE", "DELETE"))
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	f, err := NewFilter(FilterConfig{Tables: []string{"orders"}})
	if err != nil {
		t.Fatalf("NewFilter: %v", err)
	}

	process := func(m *Message) (*Message, error) {
		if err := v.Validate(m); err != nil {
			return nil, err
		}
		if !f.Match(m) {
			return nil, nil
		}
		return m, nil
	}

	// Valid message that matches the filter.
	out, err := process(&Message{Table: "orders", Action: "INSERT"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil {
		t.Fatal("expected message to pass through")
	}

	// Valid message that does NOT match the filter.
	out, err = process(&Message{Table: "users", Action: "UPDATE"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Fatal("expected message to be filtered out")
	}

	// Invalid message — empty table — should fail validation before filter.
	_, err = process(&Message{Table: "", Action: "INSERT"})
	if err == nil {
		t.Fatal("expected validation error")
	}
	var ve *ErrValidation
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ErrValidation, got %T", err)
	}
}

// TestValidator_InvalidActionBlocksMessage ensures that a message with an
// unexpected action is blocked even when the table would otherwise match.
func TestValidator_InvalidActionBlocksMessage(t *testing.T) {
	v, _ := NewValidator(RequireAction("INSERT", "UPDATE"))

	msgs := []*Message{
		{Table: "products", Action: "INSERT"},
		{Table: "products", Action: "DELETE"},
		{Table: "products", Action: "UPDATE"},
	}

	passed := 0
	for _, m := range msgs {
		if err := v.Validate(m); err == nil {
			passed++
		}
	}
	if passed != 2 {
		t.Errorf("expected 2 messages to pass, got %d", passed)
	}
}
