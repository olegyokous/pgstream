package wal

import (
	"context"
	"sync"
	"time"
)

// ThrottleConfig controls how the Throttler behaves.
type ThrottleConfig struct {
	// MessagesPerSecond is the maximum number of messages allowed per second.
	MessagesPerSecond int
	// BurstSize is the number of messages allowed to pass before throttling kicks in.
	BurstSize int
}

// DefaultThrottleConfig returns a sensible default throttle configuration.
func DefaultThrottleConfig() ThrottleConfig {
	return ThrottleConfig{
		MessagesPerSecond: 1000,
		BurstSize:         100,
	}
}

// Throttler limits the rate at which messages are processed.
type Throttler struct {
	cfg    ThrottleConfig
	mu     sync.Mutex
	tokens float64
	lastAt time.Time
	clock  func() time.Time
}

// NewThrottler creates a new Throttler with the given config.
func NewThrottler(cfg ThrottleConfig) *Throttler {
	if cfg.MessagesPerSecond <= 0 {
		cfg.MessagesPerSecond = DefaultThrottleConfig().MessagesPerSecond
	}
	if cfg.BurstSize <= 0 {
		cfg.BurstSize = DefaultThrottleConfig().BurstSize
	}
	return &Throttler{
		cfg:    cfg,
		tokens: float64(cfg.BurstSize),
		lastAt: time.Now(),
		clock:  time.Now,
	}
}

// Wait blocks until a token is available or the context is cancelled.
func (t *Throttler) Wait(ctx context.Context) error {
	for {
		t.mu.Lock()
		now := t.clock()
		elapsed := now.Sub(t.lastAt).Seconds()
		t.tokens += elapsed * float64(t.cfg.MessagesPerSecond)
		if t.tokens > float64(t.cfg.BurstSize) {
			t.tokens = float64(t.cfg.BurstSize)
		}
		t.lastAt = now
		if t.tokens >= 1 {
			t.tokens--
			t.mu.Unlock()
			return nil
		}
		t.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second / time.Duration(t.cfg.MessagesPerSecond)):
		}
	}
}

// Available returns the current number of available tokens (rounded down).
func (t *Throttler) Available() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return int(t.tokens)
}
