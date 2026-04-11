package wal

import (
	"errors"
	"sync"
	"time"
)

// WindowConfig holds configuration for the sliding window counter.
type WindowConfig struct {
	Size     int           // number of buckets
	Interval time.Duration // duration of each bucket
}

// DefaultWindowConfig returns a WindowConfig with sensible defaults.
func DefaultWindowConfig() WindowConfig {
	return WindowConfig{
		Size:     10,
		Interval: time.Second,
	}
}

// Window is a sliding-window event counter backed by fixed-size buckets.
type Window struct {
	mu      sync.Mutex
	cfg     WindowConfig
	buckets []int64
	times   []time.Time
	head    int
	clock   func() time.Time
}

// NewWindow creates a new sliding-window counter.
func NewWindow(cfg WindowConfig) (*Window, error) {
	if cfg.Size <= 0 {
		return nil, errors.New("window: size must be greater than zero")
	}
	if cfg.Interval <= 0 {
		return nil, errors.New("window: interval must be greater than zero")
	}
	w := &Window{
		cfg:     cfg,
		buckets: make([]int64, cfg.Size),
		times:   make([]time.Time, cfg.Size),
		clock:   time.Now,
	}
	now := w.clock()
	for i := range w.times {
		w.times[i] = now
	}
	return w, nil
}

// Record increments the counter for the current time bucket.
func (w *Window) Record(n int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.clock()
	w.advance(now)
	w.buckets[w.head] += n
}

// Count returns the total events within the sliding window.
func (w *Window) Count() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.clock()
	w.advance(now)
	cutoff := now.Add(-time.Duration(w.cfg.Size) * w.cfg.Interval)
	var total int64
	for i, t := range w.times {
		if t.After(cutoff) {
			total += w.buckets[i]
		}
	}
	return total
}

// advance rotates the head bucket if the current interval has elapsed.
func (w *Window) advance(now time.Time) {
	if now.Sub(w.times[w.head]) >= w.cfg.Interval {
		w.head = (w.head + 1) % w.cfg.Size
		w.buckets[w.head] = 0
		w.times[w.head] = now
	}
}
