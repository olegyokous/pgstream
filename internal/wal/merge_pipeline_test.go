package wal

import (
	"strings"
	"testing"
)

func TestMerger_IntegratesWithTransformer(t *testing.T) {
	merger, err := NewMerger(WithMergeStrategy(MergePreferSrc))
	if err != nil {
		t.Fatalf("NewMerger: %v", err)
	}

	base := &Message{
		Table:   "events",
		Action:  "INSERT",
		Columns: []Column{{Name: "id", Value: "42"}, {Name: "type", Value: "click"}},
	}
	patch := &Message{
		Table:   "events",
		Action:  "INSERT",
		Columns: []Column{{Name: "type", Value: "view"}, {Name: "user_id", Value: "7"}},
	}

	out, err := merger.Merge(base, patch)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	colMap := make(map[string]interface{}, len(out.Columns))
	for _, c := range out.Columns {
		colMap[c.Name] = c.Value
	}

	if colMap["type"] != "view" {
		t.Errorf("expected type=view after src-preferred merge, got %v", colMap["type"])
	}
	if colMap["user_id"] != "7" {
		t.Errorf("expected user_id=7 to be added, got %v", colMap["user_id"])
	}
	if colMap["id"] != "42" {
		t.Errorf("expected id=42 to be preserved, got %v", colMap["id"])
	}
}

func TestMerger_NilDstErrorMessageIsDescriptive(t *testing.T) {
	m, _ := NewMerger()
	_, err := m.Merge(nil, &Message{Table: "t"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "dst") {
		t.Errorf("error should mention 'dst', got: %v", err)
	}
}

func TestMerger_ChainedMergeAccumulatesColumns(t *testing.T) {
	m, _ := NewMerger()

	base := &Message{Table: "log", Action: "INSERT", Columns: []Column{{Name: "a", Value: "1"}}}
	s1 := &Message{Table: "log", Action: "INSERT", Columns: []Column{{Name: "b", Value: "2"}}}
	s2 := &Message{Table: "log", Action: "INSERT", Columns: []Column{{Name: "c", Value: "3"}}}

	out, err := m.Merge(base, s1)
	if err != nil {
		t.Fatalf("first merge: %v", err)
	}
	out, err = m.Merge(out, s2)
	if err != nil {
		t.Fatalf("second merge: %v", err)
	}

	if len(out.Columns) != 3 {
		t.Errorf("expected 3 columns after chained merges, got %d", len(out.Columns))
	}
}
