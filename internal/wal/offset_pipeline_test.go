package wal_test

import (
	"testing"

	"github.com/your-org/pgstream/internal/wal"
)

// TestOffsetTracker_PipelineSimulation mimics a simplified pipeline where
// messages are decoded, their LSNs are tracked, and then committed in batches.
func TestOffsetTracker_PipelineSimulation(t *testing.T) {
	type event struct {
		lsn uint64
	}

	events := []event{
		{lsn: 100},
		{lsn: 200},
		{lsn: 300},
		{lsn: 400},
		{lsn: 500},
	}

	ot := wal.NewOffsetTracker(0)

	// Simulate processing events without committing.
	for _, e := range events {
		if err := ot.Advance(e.lsn); err != nil {
			t.Fatalf("advance %d: %v", e.lsn, err)
		}
	}

	if lag := ot.Lag(); lag != 500 {
		t.Fatalf("expected lag=500 before commit, got %d", lag)
	}

	// Commit the batch.
	committed := ot.Commit()
	if committed != 500 {
		t.Fatalf("expected committed=500, got %d", committed)
	}

	if lag := ot.Lag(); lag != 0 {
		t.Fatalf("expected lag=0 after commit, got %d", lag)
	}
}

// TestOffsetTracker_ZeroLSNIsNoop ensures advancing to 0 from 0 is harmless.
func TestOffsetTracker_ZeroLSNIsNoop(t *testing.T) {
	ot := wal.NewOffsetTracker(0)
	if err := ot.Advance(0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := ot.Lag(); got != 0 {
		t.Fatalf("expected lag=0, got %d", got)
	}
}
