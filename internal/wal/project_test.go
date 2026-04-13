package wal

import (
	"testing"
)

func projectMsg() *Message {
	return &Message{
		Table:  "users",
		Action: "INSERT",
		Columns: []Column{
			{Name: "id", Value: "1"},
			{Name: "email", Value: "a@b.com"},
			{Name: "password", Value: "secret"},
			{Name: "created_at", Value: "2024-01-01"},
		},
	}
}

func TestNewProjector_MutuallyExclusiveOptions(t *testing.T) {
	_, err := NewProjector(ProjectConfig{
		Columns: []string{"id"},
		Exclude: []string{"password"},
	})
	if err == nil {
		t.Fatal("expected error for mutually exclusive options")
	}
}

func TestProjector_NilMessagePassthrough(t *testing.T) {
	p, _ := NewProjector(ProjectConfig{Columns: []string{"id"}})
	if got := p.Apply(nil); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestProjector_KeepColumns(t *testing.T) {
	p, _ := NewProjector(ProjectConfig{Columns: []string{"id", "email"}})
	out := p.Apply(projectMsg())
	if len(out.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(out.Columns))
	}
	for _, col := range out.Columns {
		if col.Name != "id" && col.Name != "email" {
			t.Errorf("unexpected column %q", col.Name)
		}
	}
}

func TestProjector_ExcludeColumns(t *testing.T) {
	p, _ := NewProjector(ProjectConfig{Exclude: []string{"password"}})
	out := p.Apply(projectMsg())
	for _, col := range out.Columns {
		if col.Name == "password" {
			t.Error("password column should have been excluded")
		}
	}
	if len(out.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(out.Columns))
	}
}

func TestProjector_NoRulesKeepsAll(t *testing.T) {
	p, _ := NewProjector(ProjectConfig{})
	out := p.Apply(projectMsg())
	if len(out.Columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(out.Columns))
	}
}

func TestProjector_TableScopedSkipsOtherTables(t *testing.T) {
	p, _ := NewProjector(ProjectConfig{
		Table:   "orders",
		Columns: []string{"id"},
	})
	out := p.Apply(projectMsg()) // table is "users", not "orders"
	if len(out.Columns) != 4 {
		t.Fatalf("expected original 4 columns for non-matching table, got %d", len(out.Columns))
	}
}

func TestProjector_OriginalMessageUnmutated(t *testing.T) {
	p, _ := NewProjector(ProjectConfig{Columns: []string{"id"}})
	orig := projectMsg()
	p.Apply(orig)
	if len(orig.Columns) != 4 {
		t.Fatal("original message was mutated")
	}
}
