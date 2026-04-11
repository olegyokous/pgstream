package wal

import (
	"testing"
	"time"
)

func TestWatermark_InitialisesAtZero(t *testing.T) {
	w := NewWatermark(DefaultWatermarkConfig())
	if w.LSN() != 0 {
		t.Fatalf("expected initial LSN 0, got %d", w.LSN())
	}
	if !w.UpdatedAt().IsZero() {
		t.Fatalf("expected zero UpdatedAt, got %s", w.UpdatedAt())
	}
}

func TestWatermark_AdvanceMovesForward(t *testing.T) {
	w := NewWatermark(DefaultWatermarkConfig())
	w.Advance(100)
	if w.LSN() != 100 {
		t.Fatalf("expected LSN 100, got %d", w.LSN())
	}
	w.Advance(200)
	if w.LSN() != 200 {
		t.Fatalf("expected LSN 200, got %d", w.LSN())
	}
}

func TestWatermark_AdvanceIgnoresOlderLSN(t *testing.T) {
	w := NewWatermark(DefaultWatermarkConfig())
	w.Advance(500)
	w.Advance(300)
	if w.LSN() != 500 {
		t.Fatalf("expected LSN 500 after backward advance, got %d", w.LSN())
	}
}

func TestWatermark_UpdatedAtSetOnAdvance(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	w := NewWatermark(DefaultWatermarkConfig())
	w.clock = func() time.Time { return now }
	w.Advance(1)
	if !w.UpdatedAt().Equal(now) {
		t.Fatalf("expected UpdatedAt %s, got %s", now, w.UpdatedAt())
	}
}

func TestWatermark_IsStale_NeverAdvanced(t *testing.T) {
	w := NewWatermark(DefaultWatermarkConfig())
	if !w.IsStale() {
		t.Fatal("expected watermark that was never advanced to be stale")
	}
}

func TestWatermark_IsStale_RecentAdvance(t *testing.T) {
	now := time.Now()
	w := NewWatermark(WatermarkConfig{StaleDuration: 10 * time.Second})
	w.clock = func() time.Time { return now }
	w.Advance(1)
	if w.IsStale() {
		t.Fatal("expected recently advanced watermark to not be stale")
	}
}

func TestWatermark_IsStale_OldAdvance(t *testing.T) {
	base := time.Now()
	w := NewWatermark(WatermarkConfig{StaleDuration: 5 * time.Second})
	w.clock = func() time.Time { return base }
	w.Advance(1)
	// Move clock past stale threshold.
	w.clock = func() time.Time { return base.Add(10 * time.Second) }
	if !w.IsStale() {
		t.Fatal("expected watermark advanced long ago to be stale")
	}
}

func TestWatermark_String(t *testing.T) {
	w := NewWatermark(DefaultWatermarkConfig())
	w.Advance(42)
	s := w.String()
	if s == "" {
		t.Fatal("expected non-empty string representation")
	}
}
