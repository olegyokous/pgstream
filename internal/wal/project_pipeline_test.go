package wal

import (
	"testing"
)

func TestProjector_IntegratesWithTransformer(t *testing.T) {
	proj, err := NewProjector(ProjectConfig{
		Columns: []string{"id", "email"},
	})
	if err != nil {
		t.Fatalf("NewProjector: %v", err)
	}

	transformer, err := NewTransformer([]TransformFunc{
		func(m *Message) *Message { return proj.Apply(m) },
	})
	if err != nil {
		t.Fatalf("NewTransformer: %v", err)
	}

	msg := &Message{
		Table:  "users",
		Action: "INSERT",
		Columns: []Column{
			{Name: "id", Value: "42"},
			{Name: "email", Value: "x@y.com"},
			{Name: "password", Value: "hunter2"},
		},
	}

	out := transformer.Apply(msg)
	if out == nil {
		t.Fatal("expected non-nil output")
	}
	if len(out.Columns) != 2 {
		t.Fatalf("expected 2 columns after projection, got %d", len(out.Columns))
	}
}

func TestProjector_ExcludeInChainWithMasker(t *testing.T) {
	proj, _ := NewProjector(ProjectConfig{
		Exclude: []string{"password"},
	})

	masker, err := NewMasker([]MaskRule{
		{Table: "users", Column: "email", Strategy: MaskRedact},
	})
	if err != nil {
		t.Fatalf("NewMasker: %v", err)
	}

	msg := &Message{
		Table:  "users",
		Action: "UPDATE",
		Columns: []Column{
			{Name: "id", Value: "7"},
			{Name: "email", Value: "user@example.com"},
			{Name: "password", Value: "s3cr3t"},
		},
	}

	after := proj.Apply(msg)
	after = masker.Apply(after)

	if after == nil {
		t.Fatal("expected non-nil")
	}
	for _, col := range after.Columns {
		if col.Name == "password" {
			t.Error("password should have been excluded")
		}
		if col.Name == "email" && col.Value == "user@example.com" {
			t.Error("email should have been masked")
		}
	}
}
