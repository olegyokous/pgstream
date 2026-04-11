package wal_test

import (
	"sync"
	"testing"

	"github.com/your-org/pgstream/internal/wal"
)

func TestOffsetTracker_ConcurrentAdvances(t *testing.T) {
	ot := wal.NewOffsetTracker(0)
	const goroutines = 50
	const lsnStep = 10

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		lsn := uint64((i + 1) * lsnStep)
		go func(l uint64) {
			defer wg.Done()
			_ = ot.Advance(l)
		}(lsn)
	}
	wg.Wait()

	expected := uint64(goroutines * lsnStep)
	if got := ot.Pending(); got != expected {
		t.Fatalf("expected pending=%d, got %d", expected, got)
	}
}

func TestOffsetTracker_CommitAndReAdvance(t *testing.T) {
	ot := wal.NewOffsetTracker(0)

	for round := 1; round <= 5; round++ {
		lsn := uint64(round * 100)
		if err := ot.Advance(lsn); err != nil {
			t.Fatalf("round %d: unexpected advance error: %v", round, err)
		}
		committed := ot.Commit()
		if committed != lsn {
			t.Fatalf("round %d: expected committed=%d, got %d", round, lsn, committed)
		}
		if lag := ot.Lag(); lag != 0 {
			t.Fatalf("round %d: expected lag=0, got %d", round, lag)
		}
	}
}
