package wal

import (
	"context"
	"testing"
	"time"
)

func TestThrottler_ImmediatelyAllowsBurstTokens(t *testing.T) {
	th := NewThrottler(ThrottleConfig{MessagesPerSecond: 10, BurstSize: 5})
	if got := th.Available(); got != 5 {
		t.Fatalf("expected 5 burst tokens, got %d", got)
	}
}

func TestThrottler_WaitConsumesToken(t *testing.T) {
	th := NewThrottler(ThrottleConfig{MessagesPerSecond: 100, BurstSize: 10})
	ctx := context.Background()
	if err := th.Wait(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := th.Available(); got != 9 {
		t.Fatalf("expected 9 tokens after one Wait, got %d", got)
	}
}

func TestThrottler_WaitRespectsContextCancellation(t *testing.T) {
	// Use a very low rate so tokens are exhausted.
	th := NewThrottler(ThrottleConfig{MessagesPerSecond: 1, BurstSize: 1})
	ctx := context.Background()
	// Drain the single burst token.
	_ = th.Wait(ctx)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := th.Wait(ctx)
	if err == nil {
		t.Fatal("expected context cancellation error, got nil")
	}
}

func TestThrottler_DefaultConfigApplied(t *testing.T) {
	th := NewThrottler(ThrottleConfig{})
	def := DefaultThrottleConfig()
	if th.cfg.MessagesPerSecond != def.MessagesPerSecond {
		t.Errorf("expected default MessagesPerSecond %d, got %d", def.MessagesPerSecond, th.cfg.MessagesPerSecond)
	}
	if th.cfg.BurstSize != def.BurstSize {
		t.Errorf("expected default BurstSize %d, got %d", def.BurstSize, th.cfg.BurstSize)
	}
}

func TestThrottler_TokensRefillOverTime(t *testing.T) {
	th := NewThrottler(ThrottleConfig{MessagesPerSecond: 100, BurstSize: 5})
	// Drain all tokens.
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = th.Wait(ctx)
	}
	if th.Available() != 0 {
		t.Fatal("expected 0 tokens after draining burst")
	}
	// Advance the internal clock by 1 second.
	th.mu.Lock()
	th.lastAt = th.lastAt.Add(-1 * time.Second)
	th.mu.Unlock()

	if err := th.Wait(ctx); err != nil {
		t.Fatalf("expected token after refill, got error: %v", err)
	}
}
