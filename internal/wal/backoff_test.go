package wal

import (
	"testing"
	"time"
)

func TestBackoff_FirstIntervalIsInitial(t *testing.T) {
	cfg := BackoffConfig{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     10 * time.Second,
		Multiplier:      2.0,
		MaxElapsedTime:  1 * time.Minute,
	}
	b := NewBackoff(cfg)

	d, ok := b.Next()
	if !ok {
		t.Fatal("expected ok=true on first call")
	}
	if d != 100*time.Millisecond {
		t.Fatalf("expected 100ms, got %v", d)
	}
}

func TestBackoff_IntervalsGrow(t *testing.T) {
	cfg := BackoffConfig{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     10 * time.Second,
		Multiplier:      2.0,
		MaxElapsedTime:  1 * time.Minute,
	}
	b := NewBackoff(cfg)

	d1, _ := b.Next()
	d2, _ := b.Next()
	if d2 <= d1 {
		t.Fatalf("expected d2 > d1, got d1=%v d2=%v", d1, d2)
	}
}

func TestBackoff_CapsAtMaxInterval(t *testing.T) {
	cfg := BackoffConfig{
		InitialInterval: 1 * time.Second,
		MaxInterval:     2 * time.Second,
		Multiplier:      10.0,
		MaxElapsedTime:  1 * time.Minute,
	}
	b := NewBackoff(cfg)

	for i := 0; i < 5; i++ {
		d, ok := b.Next()
		if !ok {
			t.Fatal("unexpected ok=false")
		}
		if d > 2*time.Second {
			t.Fatalf("interval %v exceeded max interval", d)
		}
	}
}

func TestBackoff_StopsAfterMaxElapsed(t *testing.T) {
	cfg := BackoffConfig{
		InitialInterval: 1 * time.Millisecond,
		MaxInterval:     10 * time.Millisecond,
		Multiplier:      1.5,
		MaxElapsedTime:  1 * time.Nanosecond, // effectively immediate expiry
	}
	b := NewBackoff(cfg)
	time.Sleep(2 * time.Millisecond) // ensure elapsed > MaxElapsedTime

	_, ok := b.Next()
	if ok {
		t.Fatal("expected ok=false after max elapsed time")
	}
}

func TestBackoff_ResetRestartsCounting(t *testing.T) {
	cfg := DefaultBackoffConfig()
	b := NewBackoff(cfg)

	d1, _ := b.Next()
	b.Next()
	b.Reset()
	d2, _ := b.Next()

	if d1 != d2 {
		t.Fatalf("expected same interval after reset: d1=%v d2=%v", d1, d2)
	}
}
