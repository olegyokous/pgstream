package wal

import (
	"errors"
	"testing"
)

func validateMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestNewValidator_RequiresRules(t *testing.T) {
	_, err := NewValidator()
	if err == nil {
		t.Fatal("expected error when no rules provided")
	}
}

func TestNewValidator_ValidRules(t *testing.T) {
	v, err := NewValidator(RequireTable())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v == nil {
		t.Fatal("expected non-nil validator")
	}
}

func TestValidator_NilMessageIsNoop(t *testing.T) {
	v, _ := NewValidator(RequireTable())
	if err := v.Validate(nil); err != nil {
		t.Fatalf("expected nil error for nil message, got %v", err)
	}
}

func TestValidator_PassesValidMessage(t *testing.T) {
	v, _ := NewValidator(RequireTable(), RequireAction("INSERT", "UPDATE"))
	m := validateMsg("users", "INSERT")
	if err := v.Validate(m); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidator_FailsEmptyTable(t *testing.T) {
	v, _ := NewValidator(RequireTable())
	m := validateMsg("", "INSERT")
	err := v.Validate(m)
	if err == nil {
		t.Fatal("expected validation error for empty table")
	}
	var ve *ErrValidation
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ErrValidation, got %T", err)
	}
	if ve.Rule != "require_table" {
		t.Errorf("unexpected rule name: %s", ve.Rule)
	}
}

func TestValidator_FailsDisallowedAction(t *testing.T) {
	v, _ := NewValidator(RequireAction("INSERT"))
	m := validateMsg("orders", "DELETE")
	err := v.Validate(m)
	if err == nil {
		t.Fatal("expected validation error for disallowed action")
	}
	var ve *ErrValidation
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ErrValidation, got %T", err)
	}
	if ve.Rule != "require_action" {
		t.Errorf("unexpected rule name: %s", ve.Rule)
	}
}

func TestValidator_StopsAtFirstFailingRule(t *testing.T) {
	calls := 0
	countRule := ValidationRule{
		Name: "counter",
		Predicate: func(_ *Message) error {
			calls++
			return nil
		},
	}
	v, _ := NewValidator(RequireTable(), countRule)
	v.Validate(validateMsg("", "INSERT")) //nolint:errcheck
	if calls != 0 {
		t.Errorf("expected counter rule to be skipped, called %d time(s)", calls)
	}
}
