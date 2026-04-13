package wal

import "testing"

func sumMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestNewSummarizer_StartsEmpty(t *testing.T) {
	s := NewSummarizer()
	snap := s.Snapshot()
	if snap.Total != 0 {
		t.Fatalf("expected 0 total, got %d", snap.Total)
	}
	if len(snap.Counts) != 0 {
		t.Fatalf("expected empty counts, got %v", snap.Counts)
	}
}

func TestSummarizer_RecordIncrementsCount(t *testing.T) {
	s := NewSummarizer()
	s.Record(sumMsg("users", "INSERT"))
	s.Record(sumMsg("users", "INSERT"))
	s.Record(sumMsg("users", "UPDATE"))
	snap := s.Snapshot()
	if snap.Total != 3 {
		t.Fatalf("expected total 3, got %d", snap.Total)
	}
	if snap.Counts["users"]["INSERT"] != 2 {
		t.Fatalf("expected 2 INSERTs, got %d", snap.Counts["users"]["INSERT"])
	}
	if snap.Counts["users"]["UPDATE"] != 1 {
		t.Fatalf("expected 1 UPDATE, got %d", snap.Counts["users"]["UPDATE"])
	}
}

func TestSummarizer_NilMessagePassthrough(t *testing.T) {
	s := NewSummarizer()
	if got := s.Record(nil); got != nil {
		t.Fatal("expected nil return for nil message")
	}
	if s.Snapshot().Total != 0 {
		t.Fatal("nil message should not increment total")
	}
}

func TestSummarizer_TableFilterSkipsOthers(t *testing.T) {
	s := NewSummarizer(WithSummarizerTable("orders"))
	s.Record(sumMsg("users", "INSERT"))
	s.Record(sumMsg("orders", "DELETE"))
	snap := s.Snapshot()
	if snap.Total != 1 {
		t.Fatalf("expected total 1, got %d", snap.Total)
	}
	if snap.Counts["users"] != nil {
		t.Fatal("users should not be in summary")
	}
}

func TestSummarizer_SnapshotIsImmutable(t *testing.T) {
	s := NewSummarizer()
	s.Record(sumMsg("users", "INSERT"))
	snap := s.Snapshot()
	snap.Counts["users"]["INSERT"] = 999
	snap2 := s.Snapshot()
	if snap2.Counts["users"]["INSERT"] != 1 {
		t.Fatal("snapshot mutation affected internal state")
	}
}

func TestSummarizer_ResetClearsAll(t *testing.T) {
	s := NewSummarizer()
	s.Record(sumMsg("users", "INSERT"))
	s.Reset()
	snap := s.Snapshot()
	if snap.Total != 0 {
		t.Fatalf("expected 0 after reset, got %d", snap.Total)
	}
}

func TestSummary_StringEmpty(t *testing.T) {
	s := Summary{Counts: map[string]map[string]int{}, Total: 0}
	got := s.String()
	if got != "summary: no messages" {
		t.Fatalf("unexpected string: %q", got)
	}
}
