package wal

import (
	"testing"
)

func TestSnapshotCollector_InitialisesEmpty(t *testing.T) {
	sc := NewSnapshotCollector()
	if sc.Total() != 0 {
		t.Fatalf("expected 0 total, got %d", sc.Total())
	}
}

func TestSnapshotCollector_RecordIncrements(t *testing.T) {
	sc := NewSnapshotCollector()
	sc.Record("users", "INSERT")
	sc.Record("users", "INSERT")
	sc.Record("orders", "DELETE")

	if sc.Total() != 3 {
		t.Fatalf("expected total 3, got %d", sc.Total())
	}
}

func TestSnapshotCollector_SnapshotIsImmutable(t *testing.T) {
	sc := NewSnapshotCollector()
	sc.Record("users", "UPDATE")

	snap := sc.Snapshot()
	sc.Record("users", "UPDATE") // mutate after snapshot

	if snap.Counts["users:UPDATE"] != 1 {
		t.Fatalf("expected snapshot count 1, got %d", snap.Counts["users:UPDATE"])
	}
}

func TestSnapshotCollector_Reset(t *testing.T) {
	sc := NewSnapshotCollector()
	sc.Record("users", "INSERT")
	sc.Reset()

	if sc.Total() != 0 {
		t.Fatalf("expected 0 after reset, got %d", sc.Total())
	}
}

func TestSnapshot_SummaryEmpty(t *testing.T) {
	sc := NewSnapshotCollector()
	snap := sc.Snapshot()
	if snap.Summary() != "no events recorded" {
		t.Fatalf("unexpected summary: %s", snap.Summary())
	}
}

func TestSnapshot_SummaryNonEmpty(t *testing.T) {
	sc := NewSnapshotCollector()
	sc.Record("accounts", "INSERT")
	snap := sc.Snapshot()
	summary := snap.Summary()
	if summary == "" || summary == "no events recorded" {
		t.Fatalf("expected non-empty summary, got: %s", summary)
	}
}

func TestSnapshotCollector_CapturedAtIsSet(t *testing.T) {
	sc := NewSnapshotCollector()
	snap := sc.Snapshot()
	if snap.CapturedAt.IsZero() {
		t.Fatal("expected CapturedAt to be set")
	}
}
