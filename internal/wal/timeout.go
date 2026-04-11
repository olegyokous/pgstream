package wal

import (
	"context"
	"fmt"
	"time"
)

// DefaultTimeoutConfig returns a TimeoutConfig with sensible defaults.
func DefaultTimeoutConfig() TimeoutConfig {
	return TimeoutConfig{
		Duration: 5 * time.Second,
	}
}

// TimeoutConfig controls how long an operation is allowed to run.
type TimeoutConfig struct {
	Duration time.Duration
}

// Timeouter wraps an operation with a deadline derived from the config.
type Timeouter struct {
	cfg TimeoutConfig
}

// NewTimeouter creates a Timeouter with the given config.
// If cfg.Duration is zero or negative the default duration is used.
func NewTimeouter(cfg TimeoutConfig) *Timeouter {
	if cfg.Duration <= 0 {
		cfg = DefaultTimeoutConfig()
	}
	return &Timeouter{cfg: cfg}
}

// Do runs fn within a timeout derived from t.cfg.Duration.
// It returns an error if fn exceeds the deadline or returns one itself.
func (t *Timeouter) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	tctx, cancel := context.WithTimeout(ctx, t.cfg.Duration)
	defer cancel()

	type result struct {
		err error
	}
	ch := make(chan result, 1)
	go func() {
		ch <- result{err: fn(tctx)}
	}()

	select {
	case r := <-ch:
		return r.err
	case <-tctx.Done():
		return fmt.Errorf("timeouter: operation exceeded %s: %w", t.cfg.Duration, tctx.Err())
	}
}

// Duration returns the configured timeout duration.
func (t *Timeouter) Duration() time.Duration {
	return t.cfg.Duration
}
