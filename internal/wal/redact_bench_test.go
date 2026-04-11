package wal_test

import (
	"testing"

	"pgstream/internal/wal"
)

func BenchmarkRedactor_Apply(b *testing.B) {
	r, _ := wal.NewRedactor(wal.RedactConfig{
		Columns: map[string][]string{
			"users": {"email", "password", "ssn"},
		},
	})
	msg := wal.Message{
		Table:  "users",
		Action: "INSERT",
		Columns: []wal.Column{
			{Name: "id", Value: "42"},
			{Name: "email", Value: "user@example.com"},
			{Name: "password", Value: "hunter2"},
			{Name: "ssn", Value: "000-00-0000"},
			{Name: "name", Value: "Alice"},
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Apply(msg)
	}
}

func BenchmarkRedactor_ApplyPattern(b *testing.B) {
	r, _ := wal.NewRedactor(wal.RedactConfig{
		Pattern: `\d{3}-\d{2}-\d{4}`,
	})
	msg := wal.Message{
		Table:  "records",
		Action: "INSERT",
		Columns: []wal.Column{
			{Name: "field1", Value: "123-45-6789"},
			{Name: "field2", Value: "not a ssn"},
			{Name: "field3", Value: "987-65-4321"},
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Apply(msg)
	}
}
