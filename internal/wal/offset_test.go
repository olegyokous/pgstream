package wal

import (
	"testing"
)

func TestOffsetTracker_InitialisesAtStart(t *testing.T) {
	ot := NewOffsetTracker(100)
	if got := ot.Committed(); got != 100 {
		t.Fatalf("expected committed=100, got %d", got)
	}
	if got := ot.Pending(); got != 100 {
		t.Fatalf("expected pending=100, got %d", got)
	}
}

func TestOffsetTracker_AdvanceUpdatesPending(t *testing.T) {
	ot := NewOffsetTracker(0)
	if err := ot.Advance(50); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := ot.Pending(); got != 50 {
		t.Fatalf("expected pending=50, got %d", got)
	}
	if got := ot.Committed(); got != 0 {
		t.Fatalf("committed should remain 0, got %d", got)
	}
}

func TestOffsetTracker_AdvanceIgnoresOlderLSN(t *testing.T) {
	ot := NewOffsetTracker(0)
	_ = ot.Advance(100)
	_ = ot.Advance(50) // older — should be ignored
	if got := ot.Pending(); got != 100 {
		t.Fatalf("expected pending=100, got %d", got)
	}
}

func TestOffsetTracker_AdvanceBehindCommittedErrors(t *testing.T) {
	ot := NewOffsetTracker(0)
	_ = ot.Advance(200)
	ot.Commit()
	if err := ot.Advance(100); err == nil {
		t.Fatal("expected error advancing behind committed, got nil")
	}
}

func TestOffsetTracker_CommitAdvancesCommitted(t *testing.T) {
	ot := NewOffsetTracker(0)
	_ = ot.Advance(300)
	committed := ot.Commit()
	if committed != 300 {
		t.Fatalf("expected committed=300, got %d", committed)
	}
	if got := ot.Committed(); got != 300 {
		t.Fatalf("expected Committed()=300, got %d", got)
	}
}

func TestOffsetTracker_LagIsCorrect(t *testing.T) {
	ot := NewOffsetTracker(0)
	_ = ot.Advance(500)
	if lag := ot.Lag(); lag != 500 {
		t.Fatalf("expected lag=500, got %d", lag)
	}
	ot.Commit()
	if lag := ot.Lag(); lag != 0 {
		t.Fatalf("expected lag=0 after commit, got %d", lag)
	}
}
