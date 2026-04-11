package wal

import (
	"testing"
	"time"
)

func TestWindow_InitialisesZero(t *testing.T) {
	w, err := NewWindow(DefaultWindowConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := w.Count(); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestWindow_InvalidConfig(t *testing.T) {
	_, err := NewWindow(WindowConfig{Size: 0, Interval: time.Second})
	if err == nil {
		t.Fatal("expected error for zero size")
	}
	_, err = NewWindow(WindowConfig{Size: 5, Interval: 0})
	if err == nil {
		t.Fatal("expected error for zero interval")
	}
}

func TestWindow_RecordAndCount(t *testing.T) {
	w, _ := NewWindow(WindowConfig{Size: 5, Interval: time.Second})
	w.Record(3)
	w.Record(7)
	if got := w.Count(); got != 10 {
		t.Fatalf("expected 10, got %d", got)
	}
}

func TestWindow_ExpiredBucketNotCounted(t *testing.T) {
	now := time.Now()
	w, _ := NewWindow(WindowConfig{Size: 3, Interval: 100 * time.Millisecond})
	w.clock = func() time.Time { return now }

	w.Record(5)

	// Advance time beyond window (3 buckets * 100ms = 300ms total)
	w.clock = func() time.Time { return now.Add(400 * time.Millisecond) }

	if got := w.Count(); got != 0 {
		t.Fatalf("expected 0 after expiry, got %d", got)
	}
}

func TestWindow_PartialExpiry(t *testing.T) {
	now := time.Now()
	w, _ := NewWindow(WindowConfig{Size: 5, Interval: 100 * time.Millisecond})
	w.clock = func() time.Time { return now }
	w.Record(4)

	// Advance one bucket — old record still within window
	w.clock = func() time.Time { return now.Add(150 * time.Millisecond) }
	w.Record(6)

	if got := w.Count(); got != 10 {
		t.Fatalf("expected 10, got %d", got)
	}
}

func TestWindow_DefaultConfig(t *testing.T) {
	cfg := DefaultWindowConfig()
	if cfg.Size != 10 {
		t.Errorf("expected size 10, got %d", cfg.Size)
	}
	if cfg.Interval != time.Second {
		t.Errorf("expected interval 1s, got %v", cfg.Interval)
	}
}
