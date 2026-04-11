package wal

import (
	"fmt"
	"sync"
	"time"
)

// WatermarkConfig holds configuration for the watermark tracker.
type WatermarkConfig struct {
	StaleDuration time.Duration
}

// DefaultWatermarkConfig returns sensible defaults.
func DefaultWatermarkConfig() WatermarkConfig {
	return WatermarkConfig{
		StaleDuration: 30 * time.Second,
	}
}

// Watermark tracks the high-water mark LSN for a named stream along with
// the wall-clock time it was last advanced, so callers can detect stale
// replication slots.
type Watermark struct {
	mu      sync.RWMutex
	cfg     WatermarkConfig
	lsn     uint64
	updated time.Time
	clock   func() time.Time
}

// NewWatermark creates a Watermark with the supplied config.
func NewWatermark(cfg WatermarkConfig) *Watermark {
	return &Watermark{
		cfg:   cfg,
		clock: time.Now,
	}
}

// Advance moves the high-water mark forward. It is a no-op when lsn is not
// greater than the current mark.
func (w *Watermark) Advance(lsn uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if lsn > w.lsn {
		w.lsn = lsn
		w.updated = w.clock()
	}
}

// LSN returns the current high-water mark.
func (w *Watermark) LSN() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.lsn
}

// UpdatedAt returns the time the watermark was last advanced.
func (w *Watermark) UpdatedAt() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.updated
}

// IsStale reports whether the watermark has not been advanced within the
// configured StaleDuration. A watermark that has never been advanced is
// considered stale.
func (w *Watermark) IsStale() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.updated.IsZero() {
		return true
	}
	return w.clock().Sub(w.updated) > w.cfg.StaleDuration
}

// String returns a human-readable representation.
func (w *Watermark) String() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return fmt.Sprintf("Watermark{lsn=%d, updated=%s}", w.lsn, w.updated.Format(time.RFC3339))
}
