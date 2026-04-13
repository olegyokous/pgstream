package wal

import "testing"

// TestSummarizer_RecordsReturnMessageForChaining verifies that Record returns
// the original message so it can be passed to the next stage in a pipeline.
func TestSummarizer_RecordsReturnMessageForChaining(t *testing.T) {
	s := NewSummarizer()
	msg := sumMsg("products", "UPDATE")
	out := s.Record(msg)
	if out != msg {
		t.Fatal("Record must return the original message pointer")
	}
}

// TestSummarizer_MultipleTablesInPipeline simulates a multi-table stream.
func TestSummarizer_MultipleTablesInPipeline(t *testing.T) {
	s := NewSummarizer()
	msgs := []*Message{
		sumMsg("users", "INSERT"),
		sumMsg("orders", "INSERT"),
		sumMsg("orders", "UPDATE"),
		sumMsg("products", "DELETE"),
		sumMsg("users", "DELETE"),
	}
	for _, m := range msgs {
		s.Record(m)
	}
	snap := s.Snapshot()
	if snap.Total != 5 {
		t.Fatalf("expected total 5, got %d", snap.Total)
	}
	if snap.Counts["orders"]["INSERT"] != 1 {
		t.Fatalf("expected 1 orders INSERT, got %d", snap.Counts["orders"]["INSERT"])
	}
	if snap.Counts["orders"]["UPDATE"] != 1 {
		t.Fatalf("expected 1 orders UPDATE, got %d", snap.Counts["orders"]["UPDATE"])
	}
	if snap.Counts["users"]["INSERT"] != 1 {
		t.Fatalf("expected 1 users INSERT, got %d", snap.Counts["users"]["INSERT"])
	}
}

// TestSummarizer_ResetAndReRecord verifies that after a reset the summarizer
// accumulates fresh counts.
func TestSummarizer_ResetAndReRecord(t *testing.T) {
	s := NewSummarizer()
	s.Record(sumMsg("users", "INSERT"))
	s.Record(sumMsg("users", "INSERT"))
	s.Reset()
	s.Record(sumMsg("users", "UPDATE"))
	snap := s.Snapshot()
	if snap.Total != 1 {
		t.Fatalf("expected 1 after reset+record, got %d", snap.Total)
	}
	if snap.Counts["users"]["UPDATE"] != 1 {
		t.Fatalf("expected 1 UPDATE after reset, got %d", snap.Counts["users"]["UPDATE"])
	}
	if snap.Counts["users"]["INSERT"] != 0 {
		t.Fatalf("expected 0 INSERT after reset, got %d", snap.Counts["users"]["INSERT"])
	}
}
