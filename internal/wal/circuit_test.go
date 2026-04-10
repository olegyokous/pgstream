package wal

import (
	"testing"
	"time"
)

func TestCircuitBreaker_InitiallyAllows(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	if err := cb.Allow(); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestCircuitBreaker_OpensAfterMaxFailures(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 3, OpenDuration: 10 * time.Second}
	cb := NewCircuitBreaker(cfg)

	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen, got %v", cb.State())
	}
	if err := cb.Allow(); err != ErrCircuitOpen {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestCircuitBreaker_HalfOpenAfterDuration(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 1, OpenDuration: 50 * time.Millisecond}
	cb := NewCircuitBreaker(cfg)
	cb.RecordFailure()

	if cb.State() != StateOpen {
		t.Fatal("expected open state")
	}

	time.Sleep(60 * time.Millisecond)

	if err := cb.Allow(); err != nil {
		t.Fatalf("expected nil after cooldown, got %v", err)
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %v", cb.State())
	}
}

func TestCircuitBreaker_ClosesOnSuccess(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 2, OpenDuration: 50 * time.Millisecond}
	cb := NewCircuitBreaker(cfg)
	cb.RecordFailure()
	cb.RecordFailure()

	time.Sleep(60 * time.Millisecond)
	_ = cb.Allow() // transitions to half-open

	cb.RecordSuccess()
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed after success, got %v", cb.State())
	}
}

func TestCircuitBreaker_ResetOnSuccess(t *testing.T) {
	cfg := DefaultCircuitBreakerConfig()
	cb := NewCircuitBreaker(cfg)

	for i := 0; i < cfg.MaxFailures-1; i++ {
		cb.RecordFailure()
	}
	cb.RecordSuccess()

	// failure count should be reset; one more failure should not open
	cb.RecordFailure()
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed, got %v", cb.State())
	}
}
