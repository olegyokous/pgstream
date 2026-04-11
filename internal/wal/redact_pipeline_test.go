package wal_test

import (
	"bytes"
	"strings"
	"testing"

	"pgstream/internal/wal"
)

func TestRedactor_IntegratesWithFormatter(t *testing.T) {
	r, err := wal.NewRedactor(wal.RedactConfig{
		Columns: map[string][]string{"accounts": {"token"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	msg := wal.Message{
		Table:  "accounts",
		Action: "INSERT",
		Columns: []wal.Column{
			{Name: "id", Value: "1"},
			{Name: "token", Value: "super-secret-token"},
		},
	}

	redacted := r.Apply(msg)

	fmt, err := wal.NewFormatter("json")
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := fmt.Format(&buf, redacted); err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if strings.Contains(output, "super-secret-token") {
		t.Errorf("output should not contain original token, got: %s", output)
	}
	if !strings.Contains(output, "[REDACTED]") {
		t.Errorf("output should contain [REDACTED], got: %s", output)
	}
}

func TestRedactor_TransformerChain(t *testing.T) {
	r, _ := wal.NewRedactor(wal.RedactConfig{
		Columns: map[string][]string{"users": {"email"}},
	})

	mask := wal.MaskColumns("users", "phone")
	transformer := wal.NewTransformer(mask)

	msg := wal.Message{
		Table:  "users",
		Action: "INSERT",
		Columns: []wal.Column{
			{Name: "email", Value: "alice@example.com"},
			{Name: "phone", Value: "555-1234"},
			{Name: "name", Value: "Alice"},
		},
	}

	redacted := r.Apply(msg)
	transformed, err := transformer.Apply(redacted)
	if err != nil {
		t.Fatal(err)
	}

	for _, col := range transformed.Columns {
		switch col.Name {
		case "email":
			if col.Value != "[REDACTED]" {
				t.Errorf("email should be redacted, got %v", col.Value)
			}
		case "phone":
			if col.Value != "****" {
				t.Errorf("phone should be masked, got %v", col.Value)
			}
		case "name":
			if col.Value != "Alice" {
				t.Errorf("name should be unchanged, got %v", col.Value)
			}
		}
	}
}
