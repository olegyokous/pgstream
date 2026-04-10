package wal

import (
	"context"
	"testing"
	"time"
)

func TestRateLimiter_TryAcquireConsumesToken(t *testing.T) {
	rl := NewRateLimiter(1.0)

	if !rl.TryAcquire() {
		t.Fatal("expected first TryAcquire to succeed")
	}
	if rl.TryAcquire() {
		t.Fatal("expected second TryAcquire to fail immediately")
	}
}

func TestRateLimiter_TokensRefillOverTime(t *testing.T) {
	rl := NewRateLimiter(100.0) // 100 rps => 1 token per 10ms

	// Drain initial tokens
	for rl.TryAcquire() {
	}

	time.Sleep(50 * time.Millisecond)

	if !rl.TryAcquire() {
		t.Fatal("expected token to be available after refill period")
	}
}

func TestRateLimiter_WaitSucceedsWithinDeadline(t *testing.T) {
	rl := NewRateLimiter(200.0) // fast refill

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if err := rl.Wait(ctx); err != nil {
		t.Fatalf("expected Wait to succeed, got: %v", err)
	}
}

func TestRateLimiter_WaitRespectsContextCancellation(t *testing.T) {
	rl := NewRateLimiter(0.0001) // extremely slow refill

	// Drain the bucket
	rl.TryAcquire()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := rl.Wait(ctx)
	if err == nil {
		t.Fatal("expected Wait to return error on context cancellation")
	}
}

func TestNewRateLimiter_DefaultConfig(t *testing.T) {
	rl := NewRateLimiter(10.0)
	if rl == nil {
		t.Fatal("expected non-nil RateLimiter")
	}
}
