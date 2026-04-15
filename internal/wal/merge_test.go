package wal

import (
	"testing"
)

func mergeMsg(table string, cols ...Column) *Message {
	return &Message{Table: table, Action: "INSERT", Columns: cols}
}

func TestNewMerger_DefaultStrategy(t *testing.T) {
	m, err := NewMerger()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.strategy != MergePreferSrc {
		t.Errorf("expected MergePreferSrc, got %v", m.strategy)
	}
}

func TestMerger_NilSrcReturnsDst(t *testing.T) {
	m, _ := NewMerger()
	dst := mergeMsg("users", Column{Name: "id", Value: "1"})
	out, err := m.Merge(dst, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != dst {
		t.Errorf("expected same dst pointer")
	}
}

func TestMerger_NilDstReturnsError(t *testing.T) {
	m, _ := NewMerger()
	src := mergeMsg("users", Column{Name: "id", Value: "1"})
	_, err := m.Merge(nil, src)
	if err == nil {
		t.Fatal("expected error for nil dst")
	}
}

func TestMerger_PreferSrcOverwritesConflict(t *testing.T) {
	m, _ := NewMerger(WithMergeStrategy(MergePreferSrc))
	dst := mergeMsg("orders", Column{Name: "status", Value: "pending"})
	src := mergeMsg("orders", Column{Name: "status", Value: "shipped"})
	out, err := m.Merge(dst, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Columns[0].Value != "shipped" {
		t.Errorf("expected 'shipped', got %v", out.Columns[0].Value)
	}
}

func TestMerger_PreferDstKeepsConflict(t *testing.T) {
	m, _ := NewMerger(WithMergeStrategy(MergePreferDst))
	dst := mergeMsg("orders", Column{Name: "status", Value: "pending"})
	src := mergeMsg("orders", Column{Name: "status", Value: "shipped"})
	out, err := m.Merge(dst, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Columns[0].Value != "pending" {
		t.Errorf("expected 'pending', got %v", out.Columns[0].Value)
	}
}

func TestMerger_AddsNewColumnsFromSrc(t *testing.T) {
	m, _ := NewMerger()
	dst := mergeMsg("users", Column{Name: "id", Value: "1"})
	src := mergeMsg("users", Column{Name: "email", Value: "a@b.com"})
	out, err := m.Merge(dst, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(out.Columns))
	}
}

func TestMerger_TableConstraintSkipsNonMatch(t *testing.T) {
	m, _ := NewMerger(WithMergeTable("users"))
	dst := mergeMsg("orders", Column{Name: "id", Value: "1"})
	src := mergeMsg("orders", Column{Name: "total", Value: "99"})
	out, err := m.Merge(dst, src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.Columns) != 1 {
		t.Errorf("expected no merge for non-matching table, got %d columns", len(out.Columns))
	}
}
