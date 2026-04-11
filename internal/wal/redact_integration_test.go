package wal_test

import (
	"testing"

	"pgstream/internal/wal"
)

func TestRedactor_CombinedColumnAndPattern(t *testing.T) {
	r, err := wal.NewRedactor(wal.RedactConfig{
		Columns:     map[string][]string{"users": {"password"}},
		Pattern:     `^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$`,
		Replacement: "<hidden>",
	})
	if err != nil {
		t.Fatal(err)
	}
	msg := wal.Message{
		Table:  "users",
		Action: "INSERT",
		Columns: []wal.Column{
			{Name: "email", Value: "bob@example.com"},
			{Name: "password", Value: "s3cr3t"},
			{Name: "username", Value: "bob"},
		},
	}
	out := r.Apply(msg)
	if out.Columns[0].Value != "<hidden>" {
		t.Errorf("email: expected <hidden>, got %v", out.Columns[0].Value)
	}
	if out.Columns[1].Value != "<hidden>" {
		t.Errorf("password: expected <hidden>, got %v", out.Columns[1].Value)
	}
	if out.Columns[2].Value != "bob" {
		t.Errorf("username: expected bob, got %v", out.Columns[2].Value)
	}
}

func TestRedactor_OriginalMessageUnmutated(t *testing.T) {
	r, _ := wal.NewRedactor(wal.RedactConfig{
		Columns: map[string][]string{"t": {"secret"}},
	})
	orig := wal.Message{
		Table:  "t",
		Action: "UPDATE",
		Columns: []wal.Column{
			{Name: "secret", Value: "topsecret"},
		},
	}
	_ = r.Apply(orig)
	if orig.Columns[0].Value != "topsecret" {
		t.Errorf("original message was mutated")
	}
}
