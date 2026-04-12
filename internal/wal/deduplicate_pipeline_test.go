package wal

import (
	"testing"
	"time"
)

// TestContentHasher_PipelineDropsDuplicates simulates a pipeline where
// duplicate messages are filtered before being forwarded to a sink.
func TestContentHasher_PipelineDropsDuplicates(t *testing.T) {
	h, err := NewContentHasher(ContentHasherConfig{
		TTL:     time.Minute,
		MaxSize: 100,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	input := []*Message{
		{Table: "orders", Action: "INSERT", Columns: []Column{{Name: "id", Value: 1}}},
		{Table: "orders", Action: "INSERT", Columns: []Column{{Name: "id", Value: 1}}}, // dup
		{Table: "orders", Action: "INSERT", Columns: []Column{{Name: "id", Value: 2}}},
		{Table: "orders", Action: "INSERT", Columns: []Column{{Name: "id", Value: 1}}}, // dup
		{Table: "orders", Action: "UPDATE", Columns: []Column{{Name: "id", Value: 1}}}, // different action
	}

	var passed []*Message
	for _, msg := range input {
		if !h.IsDuplicate(msg) {
			passed = append(passed, msg)
		}
	}

	if len(passed) != 3 {
		t.Fatalf("expected 3 unique messages, got %d", len(passed))
	}
}

// TestContentHasher_ColumnScopedPipeline verifies that column-scoped
// hashing integrates correctly with message processing.
func TestContentHasher_ColumnScopedPipeline(t *testing.T) {
	h, _ := NewContentHasher(ContentHasherConfig{
		TTL:     time.Minute,
		MaxSize: 100,
		Columns: []string{"email"},
	})

	msgs := []*Message{
		{Table: "users", Action: "INSERT", Columns: []Column{
			{Name: "id", Value: 1}, {Name: "email", Value: "a@b.com"},
		}},
		{Table: "users", Action: "INSERT", Columns: []Column{
			{Name: "id", Value: 2}, {Name: "email", Value: "a@b.com"}, // same email => dup
		}},
		{Table: "users", Action: "INSERT", Columns: []Column{
			{Name: "id", Value: 3}, {Name: "email", Value: "c@d.com"}, // new email
		}},
	}

	var passed []*Message
	for _, m := range msgs {
		if !h.IsDuplicate(m) {
			passed = append(passed, m)
		}
	}

	if len(passed) != 2 {
		t.Fatalf("expected 2 passed messages, got %d", len(passed))
	}
}
