package wal

import (
	"testing"
)

// TestSnapshotCollector_RecordsFromMessage verifies that a WAL message's table
// and action fields map correctly into the collector.
func TestSnapshotCollector_RecordsFromMessage(t *testing.T) {
	sc := NewSnapshotCollector()

	msgs := []Message{
		{Table: "users", Action: "INSERT"},
		{Table: "users", Action: "INSERT"},
		{Table: "users", Action: "UPDATE"},
		{Table: "orders", Action: "DELETE"},
	}

	for _, m := range msgs {
		sc.Record(m.Table, m.Action)
	}

	snap := sc.Snapshot()

	cases := []struct {
		table, action string
		want          int64
	}{
		{"users", "INSERT", 2},
		{"users", "UPDATE", 1},
		{"orders", "DELETE", 1},
	}

	for _, tc := range cases {
		key := tc.table + ":" + tc.action
		if snap.Counts[key] != tc.want {
			t.Errorf("key %s: want %d, got %d", key, tc.want, snap.Counts[key])
		}
	}
}

func TestSnapshotCollector_ResetAndReRecord(t *testing.T) {
	sc := NewSnapshotCollector()
	sc.Record("users", "INSERT")
	sc.Record("users", "INSERT")
	sc.Reset()
	sc.Record("users", "DELETE")

	snap := sc.Snapshot()
	if snap.Counts["users:INSERT"] != 0 {
		t.Errorf("expected 0 INSERT after reset, got %d", snap.Counts["users:INSERT"])
	}
	if snap.Counts["users:DELETE"] != 1 {
		t.Errorf("expected 1 DELETE, got %d", snap.Counts["users:DELETE"])
	}
}
