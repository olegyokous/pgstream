package wal

import (
	"context"
	"sync"
	"time"
)

// RateLimiter enforces a maximum number of events per second using a token
// bucket approach.
type RateLimiter struct {
	mu       sync.Mutex
	tokens   float64
	max      float64
	rate     float64 // tokens per nanosecond
	lastTick time.Time
}

// NewRateLimiter creates a RateLimiter allowing up to rps events per second.
func NewRateLimiter(rps float64) *RateLimiter {
	return &RateLimiter{
		tokens:   rps,
		max:      rps,
		rate:     rps / float64(time.Second),
		lastTick: time.Now(),
	}
}

// Wait blocks until a token is available or the context is cancelled.
func (r *RateLimiter) Wait(ctx context.Context) error {
	for {
		r.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(r.lastTick)
		r.tokens += float64(elapsed) * r.rate
		if r.tokens > r.max {
			r.tokens = r.max
		}
		r.lastTick = now

		if r.tokens >= 1.0 {
			r.tokens--
			r.mu.Unlock()
			return nil
		}
		r.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(1.0/r.rate) / 2):
		}
	}
}

// TryAcquire attempts to consume a token without blocking.
// Returns true if a token was available.
func (r *RateLimiter) TryAcquire() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(r.lastTick)
	r.tokens += float64(elapsed) * r.rate
	if r.tokens > r.max {
		r.tokens = r.max
	}
	r.lastTick = now

	if r.tokens >= 1.0 {
		r.tokens--
		return true
	}
	return false
}
