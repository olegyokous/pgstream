package wal

import (
	"testing"
)

func redactMsg(table string, cols []Column) Message {
	return Message{Table: table, Action: "INSERT", Columns: cols}
}

func TestRedactor_InvalidPattern(t *testing.T) {
	_, err := NewRedactor(RedactConfig{Pattern: "[invalid"})
	if err == nil {
		t.Fatal("expected error for invalid regex, got nil")
	}
}

func TestRedactor_DefaultReplacement(t *testing.T) {
	r, err := NewRedactor(RedactConfig{
		Columns: map[string][]string{"users": {"email"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	msg := redactMsg("users", []Column{{Name: "email", Value: "alice@example.com"}})
	out := r.Apply(msg)
	if out.Columns[0].Value != "[REDACTED]" {
		t.Fatalf("expected [REDACTED], got %v", out.Columns[0].Value)
	}
}

func TestRedactor_CustomReplacement(t *testing.T) {
	r, _ := NewRedactor(RedactConfig{
		Columns:     map[string][]string{"users": {"ssn"}},
		Replacement: "***",
	})
	msg := redactMsg("users", []Column{{Name: "ssn", Value: "123-45-6789"}})
	out := r.Apply(msg)
	if out.Columns[0].Value != "***" {
		t.Fatalf("expected ***, got %v", out.Columns[0].Value)
	}
}

func TestRedactor_NonMatchingTableIsUnchanged(t *testing.T) {
	r, _ := NewRedactor(RedactConfig{
		Columns: map[string][]string{"users": {"email"}},
	})
	msg := redactMsg("orders", []Column{{Name: "email", Value: "alice@example.com"}})
	out := r.Apply(msg)
	if out.Columns[0].Value != "alice@example.com" {
		t.Fatalf("expected original value, got %v", out.Columns[0].Value)
	}
}

func TestRedactor_PatternRedactsMatchingValues(t *testing.T) {
	r, _ := NewRedactor(RedactConfig{
		Pattern: `\d{3}-\d{2}-\d{4}`,
	})
	msg := redactMsg("any", []Column{
		{Name: "ssn", Value: "123-45-6789"},
		{Name: "name", Value: "Alice"},
	})
	out := r.Apply(msg)
	if out.Columns[0].Value != "[REDACTED]" {
		t.Fatalf("expected ssn redacted, got %v", out.Columns[0].Value)
	}
	if out.Columns[1].Value != "Alice" {
		t.Fatalf("expected name unchanged, got %v", out.Columns[1].Value)
	}
}

func TestRedactor_NonStringValuesUnchanged(t *testing.T) {
	r, _ := NewRedactor(RedactConfig{
		Columns: map[string][]string{"t": {"age"}},
	})
	msg := redactMsg("t", []Column{{Name: "age", Value: 42}})
	out := r.Apply(msg)
	if out.Columns[0].Value != 42 {
		t.Fatalf("expected 42, got %v", out.Columns[0].Value)
	}
}
