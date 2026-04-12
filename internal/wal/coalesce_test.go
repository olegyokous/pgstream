package wal

import (
	"testing"
)

func coalesceMsg(table, key, val string) *Message {
	return &Message{
		Table:   table,
		Action:  "UPDATE",
		Columns: map[string]any{"id": key, "data": val},
		LSN:     1,
	}
}

func TestNewCoalescer_EmptyKeyErrors(t *testing.T) {
	_, err := NewCoalescer(CoalesceConfig{KeyColumn: ""})
	if err == nil {
		t.Fatal("expected error for empty KeyColumn")
	}
}

func TestNewCoalescer_ValidKey(t *testing.T) {
	c, err := NewCoalescer(CoalesceConfig{KeyColumn: "id"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil Coalescer")
	}
}

func TestCoalescer_MergeNilSrcReturnsDst(t *testing.T) {
	c, _ := NewCoalescer(CoalesceConfig{KeyColumn: "id"})
	dst := coalesceMsg("users", "1", "original")
	result := c.Merge(dst, nil)
	if result != dst {
		t.Fatal("expected dst returned unchanged")
	}
}

func TestCoalescer_MergeDifferentTableReturnsDst(t *testing.T) {
	c, _ := NewCoalescer(CoalesceConfig{KeyColumn: "id"})
	dst := coalesceMsg("users", "1", "original")
	src := coalesceMsg("orders", "1", "new")
	result := c.Merge(dst, src)
	if result.Columns["data"] != "original" {
		t.Fatalf("expected original value, got %v", result.Columns["data"])
	}
}

func TestCoalescer_MergeOverwritesColumns(t *testing.T) {
	c, _ := NewCoalescer(CoalesceConfig{KeyColumn: "id"})
	dst := coalesceMsg("users", "1", "old")
	dst.LSN = 10
	src := coalesceMsg("users", "1", "new")
	src.LSN = 20
	result := c.Merge(dst, src)
	if result.Columns["data"] != "new" {
		t.Fatalf("expected 'new', got %v", result.Columns["data"])
	}
	if result.LSN != 20 {
		t.Fatalf("expected LSN 20, got %v", result.LSN)
	}
}

func TestCoalescer_CoalesceSliceDeduplicates(t *testing.T) {
	c, _ := NewCoalescer(CoalesceConfig{KeyColumn: "id"})
	msgs := []*Message{
		coalesceMsg("users", "1", "a"),
		coalesceMsg("users", "2", "b"),
		coalesceMsg("users", "1", "c"),
	}
	out := c.CoalesceSlice(msgs)
	if len(out) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(out))
	}
	if out[0].Columns["data"] != "c" {
		t.Fatalf("expected last-write value 'c', got %v", out[0].Columns["data"])
	}
}

func TestCoalescer_CoalesceSliceRespectsTableFilter(t *testing.T) {
	c, _ := NewCoalescer(CoalesceConfig{KeyColumn: "id", Tables: []string{"users"}})
	msgs := []*Message{
		coalesceMsg("orders", "1", "x"),
		coalesceMsg("orders", "1", "y"),
	}
	out := c.CoalesceSlice(msgs)
	// orders is not in the filter, so both messages pass through unchanged
	if len(out) != 2 {
		t.Fatalf("expected 2 messages for unfiltered table, got %d", len(out))
	}
}

func TestCoalescer_CoalesceSliceNilsSkipped(t *testing.T) {
	c, _ := NewCoalescer(CoalesceConfig{KeyColumn: "id"})
	msgs := []*Message{nil, coalesceMsg("users", "1", "v"), nil}
	out := c.CoalesceSlice(msgs)
	if len(out) != 1 {
		t.Fatalf("expected 1 message, got %d", len(out))
	}
}
