package wal

import (
	"testing"
)

// TestPinner_BypassesFilterForPinnedTable verifies that a pinned message
// is detected as pinned even after passing through a Filter that would
// normally drop it.
func TestPinner_BypassesFilterForPinnedTable(t *testing.T) {
	pinner, _ := NewPinner()
	pinner.Pin("critical")

	f, err := NewFilter(WithTables("allowed"))
	if err != nil {
		t.Fatalf("filter: %v", err)
	}

	msgs := []*Message{
		{Table: "critical", Action: "DELETE"},
		{Table: "allowed", Action: "INSERT"},
		{Table: "other", Action: "UPDATE"},
	}

	var passed []*Message
	for _, m := range msgs {
		if pinner.IsPinned(m) || f.Match(m) {
			passed = append(passed, m)
		}
	}

	if len(passed) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(passed))
	}
	if passed[0].Table != "critical" {
		t.Errorf("expected first passed message to be critical, got %s", passed[0].Table)
	}
	if passed[1].Table != "allowed" {
		t.Errorf("expected second passed message to be allowed, got %s", passed[1].Table)
	}
}

// TestPinner_UnpinnedTableObeysFiler ensures unpinned tables respect filter.
func TestPinner_UnpinnedTableObeysFilter(t *testing.T) {
	pinner, _ := NewPinner()

	f, err := NewFilter(WithTables("allowed"))
	if err != nil {
		t.Fatalf("filter: %v", err)
	}

	m := &Message{Table: "blocked", Action: "INSERT"}
	if pinner.IsPinned(m) || f.Match(m) {
		t.Fatal("blocked table should not pass")
	}
}
