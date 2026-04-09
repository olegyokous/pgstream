package wal

import (
	"context"
	"errors"
	"time"
)

// RetryConfig holds configuration for retry behaviour.
type RetryConfig struct {
	MaxAttempts int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

// DefaultRetryConfig returns a sensible default retry configuration.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:  5,
		InitialDelay: 200 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		Multiplier:   2.0,
	}
}

// Retry executes fn up to cfg.MaxAttempts times, backing off exponentially
// between attempts. It stops early if ctx is cancelled or fn returns a
// non-retryable error wrapped with ErrNoRetry.
func Retry(ctx context.Context, cfg RetryConfig, fn func() error) error {
	delay := cfg.InitialDelay
	var lastErr error

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		var noRetry *NoRetryError
		if errors.As(lastErr, &noRetry) {
			return noRetry.Unwrap()
		}

		if attempt == cfg.MaxAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		delay = time.Duration(float64(delay) * cfg.Multiplier)
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
	}

	return lastErr
}

// NoRetryError wraps an error to signal that Retry should not attempt again.
type NoRetryError struct {
	cause error
}

func (e *NoRetryError) Error() string { return e.cause.Error() }
func (e *NoRetryError) Unwrap() error { return e.cause }

// Permanent wraps err so that Retry stops immediately.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &NoRetryError{cause: err}
}
