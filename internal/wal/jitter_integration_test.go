package wal_test

import (
	"sync"
	"testing"
	"time"

	"pgstream/internal/wal"
)

func TestJitterer_ConcurrentApplyIsSafe(t *testing.T) {
	j := wal.NewJitterer(wal.DefaultJitterConfig())
	base := 100 * time.Millisecond
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				got := j.Apply(base)
				if got < 0 {
					t.Errorf("negative jitter: %v", got)
				}
			}
		}()
	}
	wg.Wait()
}

func TestJitterer_SpreadIsNonTrivial(t *testing.T) {
	j := wal.NewJitterer(wal.JitterConfig{Factor: 0.5})
	base := 1 * time.Second
	seen := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		seen[j.Apply(base)] = true
	}
	// With factor=0.5 and 100 samples we expect meaningful spread
	if len(seen) < 10 {
		t.Fatalf("expected diverse jitter values, got only %d unique", len(seen))
	}
}
