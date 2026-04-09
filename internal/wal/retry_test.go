package wal

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errTransient = errors.New("transient error")
var errFatal = errors.New("fatal error")

func TestRetry_SucceedsOnFirstAttempt(t *testing.T) {
	calls := 0
	err := Retry(context.Background(), DefaultRetryConfig(), func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetry_RetriesAndEventuallySucceeds(t *testing.T) {
	calls := 0
	cfg := RetryConfig{MaxAttempts: 5, InitialDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond, Multiplier: 2.0}
	err := Retry(context.Background(), cfg, func() error {
		calls++
		if calls < 3 {
			return errTransient
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetry_ExhaustsAttempts(t *testing.T) {
	cfg := RetryConfig{MaxAttempts: 3, InitialDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond, Multiplier: 1.5}
	calls := 0
	err := Retry(context.Background(), cfg, func() error {
		calls++
		return errTransient
	})
	if !errors.Is(err, errTransient) {
		t.Fatalf("expected errTransient, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetry_StopsOnPermanentError(t *testing.T) {
	cfg := RetryConfig{MaxAttempts: 5, InitialDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond, Multiplier: 2.0}
	calls := 0
	err := Retry(context.Background(), cfg, func() error {
		calls++
		return Permanent(errFatal)
	})
	if !errors.Is(err, errFatal) {
		t.Fatalf("expected errFatal, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetry_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := RetryConfig{MaxAttempts: 5, InitialDelay: 50 * time.Millisecond, MaxDelay: time.Second, Multiplier: 2.0}
	calls := 0
	err := Retry(ctx, cfg, func() error {
		calls++
		return errTransient
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call before cancel, got %d", calls)
	}
}
