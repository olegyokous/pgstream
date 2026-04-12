package wal_test

import (
	"testing"
	"time"

	"pgstream/internal/wal"
)

// TestJitterer_UsedWithBackoff verifies that a Jitterer can be composed with
// the Backoff type to spread retry intervals.
func TestJitterer_UsedWithBackoff(t *testing.T) {
	bCfg := wal.DefaultBackoffConfig()
	bCfg.InitialInterval = 100 * time.Millisecond
	bCfg.Multiplier = 2.0
	bCfg.MaxInterval = 2 * time.Second
	bCfg.MaxElapsed = 10 * time.Second
	b := wal.NewBackoff(bCfg)

	j := wal.NewJitterer(wal.JitterConfig{Factor: 0.25})

	var prev time.Duration
	for i := 0; i < 5; i++ {
		raw := b.Next()
		if raw <= 0 {
			break
		}
		applied := j.Apply(raw)
		if applied < 0 {
			t.Fatalf("jittered backoff is negative: %v", applied)
		}
		_ = prev
		prev = applied
	}
}

// TestJitterer_ApplyPositiveWithRetry ensures ApplyPositive never produces a
// zero sleep even when jitter would otherwise underflow.
func TestJitterer_ApplyPositiveWithRetry(t *testing.T) {
	j := wal.NewJitterer(wal.JitterConfig{Factor: 0.99})
	minFloor := 1 * time.Millisecond
	base := 1 * time.Millisecond
	for i := 0; i < 50; i++ {
		got := j.ApplyPositive(base, minFloor)
		if got < minFloor {
			t.Fatalf("sleep below floor: %v", got)
		}
	}
}
