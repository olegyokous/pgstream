package wal

import (
	"math"
	"time"
)

// BackoffConfig holds configuration for exponential backoff.
type BackoffConfig struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	MaxElapsedTime  time.Duration
}

// DefaultBackoffConfig returns a sensible default backoff configuration.
func DefaultBackoffConfig() BackoffConfig {
	return BackoffConfig{
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     30 * time.Second,
		Multiplier:      1.5,
		MaxElapsedTime:  5 * time.Minute,
	}
}

// Backoff computes successive wait durations using exponential backoff.
type Backoff struct {
	cfg     BackoffConfig
	attempt int
	start   time.Time
}

// NewBackoff creates a new Backoff instance using the given config.
func NewBackoff(cfg BackoffConfig) *Backoff {
	return &Backoff{
		cfg:   cfg,
		start: time.Now(),
	}
}

// Next returns the next wait duration and whether the caller should continue.
// Returns false when MaxElapsedTime has been exceeded.
func (b *Backoff) Next() (time.Duration, bool) {
	if b.cfg.MaxElapsedTime > 0 && time.Since(b.start) >= b.cfg.MaxElapsedTime {
		return 0, false
	}

	interval := float64(b.cfg.InitialInterval) * math.Pow(b.cfg.Multiplier, float64(b.attempt))
	if interval > float64(b.cfg.MaxInterval) {
		interval = float64(b.cfg.MaxInterval)
	}

	b.attempt++
	return time.Duration(interval), true
}

// Reset resets the backoff to its initial state.
func (b *Backoff) Reset() {
	b.attempt = 0
	b.start = time.Now()
}
