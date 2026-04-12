package wal

import (
	"testing"
)

func aggMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestNewAggregator_InvalidGroupBy(t *testing.T) {
	_, err := NewAggregator(AggregateConfig{GroupBy: "schema"})
	if err == nil {
		t.Fatal("expected error for unsupported GroupBy, got nil")
	}
}

func TestNewAggregator_ValidGroupBy(t *testing.T) {
	for _, g := range []string{"table", "action"} {
		_, err := NewAggregator(AggregateConfig{GroupBy: g})
		if err != nil {
			t.Errorf("GroupBy=%q: unexpected error: %v", g, err)
		}
	}
}

func TestAggregator_GroupByTable(t *testing.T) {
	a, _ := NewAggregator(DefaultAggregateConfig())
	a.Record(aggMsg("users", "INSERT"))
	a.Record(aggMsg("users", "UPDATE"))
	a.Record(aggMsg("orders", "INSERT"))

	snap := a.Snapshot()
	if snap["users"] != 2 {
		t.Errorf("expected users=2, got %d", snap["users"])
	}
	if snap["orders"] != 1 {
		t.Errorf("expected orders=1, got %d", snap["orders"])
	}
}

func TestAggregator_GroupByAction(t *testing.T) {
	a, _ := NewAggregator(AggregateConfig{GroupBy: "action"})
	a.Record(aggMsg("users", "INSERT"))
	a.Record(aggMsg("orders", "INSERT"))
	a.Record(aggMsg("users", "DELETE"))

	snap := a.Snapshot()
	if snap["INSERT"] != 2 {
		t.Errorf("expected INSERT=2, got %d", snap["INSERT"])
	}
	if snap["DELETE"] != 1 {
		t.Errorf("expected DELETE=1, got %d", snap["DELETE"])
	}
}

func TestAggregator_NilMessageIgnored(t *testing.T) {
	a, _ := NewAggregator(DefaultAggregateConfig())
	a.Record(nil)
	if len(a.Snapshot()) != 0 {
		t.Error("expected empty snapshot after nil record")
	}
}

func TestAggregator_Reset(t *testing.T) {
	a, _ := NewAggregator(DefaultAggregateConfig())
	a.Record(aggMsg("users", "INSERT"))
	a.Reset()
	if len(a.Snapshot()) != 0 {
		t.Error("expected empty snapshot after reset")
	}
}

func TestAggregator_SnapshotIsImmutable(t *testing.T) {
	a, _ := NewAggregator(DefaultAggregateConfig())
	a.Record(aggMsg("users", "INSERT"))
	snap := a.Snapshot()
	snap["users"] = 999
	if a.Snapshot()["users"] != 1 {
		t.Error("mutating snapshot should not affect aggregator state")
	}
}
