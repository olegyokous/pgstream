package wal_test

import (
	"testing"

	"github.com/your-org/pgstream/internal/wal"
)

func TestPrioritizer_MixedRulesSort(t *testing.T) {
	p, err := wal.NewPrioritizer(wal.PriorityLow, []wal.PriorityRule{
		{Table: "payments", Action: "INSERT", Priority: wal.PriorityHigh},
		{Table: "events", Priority: wal.PriorityNormal},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msgs := []*wal.Message{
		{Table: "events", Action: "UPDATE"},
		{Table: "noise", Action: "DELETE"},
		{Table: "payments", Action: "INSERT"},
		{Table: "noise", Action: "INSERT"},
	}

	p.Sort(msgs)

	if msgs[0].Table != "payments" {
		t.Errorf("expected payments first, got %s", msgs[0].Table)
	}
	if msgs[1].Table != "events" {
		t.Errorf("expected events second, got %s", msgs[1].Table)
	}
	for _, m := range msgs[2:] {
		if p.Assign(m) != wal.PriorityLow {
			t.Errorf("expected low priority for %s", m.Table)
		}
	}
}

func TestPrioritizer_AllSamePriorityPreservesOrder(t *testing.T) {
	p, _ := wal.NewPrioritizer(wal.PriorityNormal, []wal.PriorityRule{
		{Table: "any", Priority: wal.PriorityNormal},
	})

	tables := []string{"a", "b", "c", "d"}
	msgs := make([]*wal.Message, len(tables))
	for i, tbl := range tables {
		msgs[i] = &wal.Message{Table: tbl, Action: "INSERT"}
	}

	p.Sort(msgs)

	for i, tbl := range tables {
		if msgs[i].Table != tbl {
			t.Errorf("position %d: expected %s, got %s", i, tbl, msgs[i].Table)
		}
	}
}
