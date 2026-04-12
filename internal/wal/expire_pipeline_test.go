package wal_test

import (
	"testing"
	"time"

	"pgstream/internal/wal"
)

func TestExpirer_DropsOldMessagesInChain(t *testing.T) {
	now := time.Date(2024, 3, 15, 9, 0, 0, 0, time.UTC)

	e, err := wal.NewExpirer(wal.ExpirerConfig{TTL: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}

	input := []*wal.Message{
		{Table: "payments", Action: "INSERT", WalTimestamp: now.Add(-5 * time.Second)},
		{Table: "payments", Action: "INSERT", WalTimestamp: now.Add(-45 * time.Second)},
		{Table: "payments", Action: "UPDATE", WalTimestamp: now.Add(-15 * time.Second)},
		{Table: "payments", Action: "DELETE", WalTimestamp: now.Add(-60 * time.Second)},
	}

	var out []*wal.Message
	for _, m := range input {
		if r := e.Apply(m); r != nil {
			out = append(out, r)
		}
	}

	if len(out) != 2 {
		t.Fatalf("expected 2 fresh messages, got %d", len(out))
	}
	for _, m := range out {
		if now.Sub(m.WalTimestamp) > 30*time.Second {
			t.Errorf("message %+v should have been dropped", m)
		}
	}
}

func TestExpirer_TableScopedLeavesOtherTablesAlone(t *testing.T) {
	now := time.Date(2024, 3, 15, 9, 0, 0, 0, time.UTC)

	e, _ := wal.NewExpirer(wal.ExpirerConfig{
		TTL:   10 * time.Second,
		Table: "sessions",
	})

	msgs := []*wal.Message{
		// old sessions message — should be dropped
		{Table: "sessions", Action: "INSERT", WalTimestamp: now.Add(-30 * time.Second)},
		// old users message — should NOT be dropped (different table)
		{Table: "users", Action: "INSERT", WalTimestamp: now.Add(-30 * time.Second)},
		// fresh sessions message — should pass
		{Table: "sessions", Action: "UPDATE", WalTimestamp: now.Add(-5 * time.Second)},
	}

	var out []*wal.Message
	for _, m := range msgs {
		if r := e.Apply(m); r != nil {
			out = append(out, r)
		}
	}

	if len(out) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(out))
	}
}
