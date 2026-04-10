package wal

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCircuitBreaker_ConcurrentFailures(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 10, OpenDuration: 5 * time.Second}
	cb := NewCircuitBreaker(cfg)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cb.RecordFailure()
		}()
	}
	wg.Wait()

	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen after concurrent failures, got %v", cb.State())
	}
}

func TestCircuitBreaker_ProtectsFunctionCall(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 2, OpenDuration: 100 * time.Millisecond}
	cb := NewCircuitBreaker(cfg)

	sentinel := errors.New("downstream error")
	call := func() error {
		if err := cb.Allow(); err != nil {
			return err
		}
		cb.RecordFailure()
		return sentinel
	}

	var errs []error
	for i := 0; i < 5; i++ {
		errs = append(errs, call())
	}

	openCount := 0
	for _, e := range errs {
		if errors.Is(e, ErrCircuitOpen) {
			openCount++
		}
	}
	if openCount == 0 {
		t.Fatal("expected at least one ErrCircuitOpen in responses")
	}
}
