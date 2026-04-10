package wal

import (
	"errors"
	"testing"
	"time"
)

// wrappedSink simulates a sink protected by a circuit breaker.
type wrappedSink struct {
	cb      *CircuitBreaker
	writes  int
	failOn  int
}

func (w *wrappedSink) Write(msg Message) error {
	if err := w.cb.Allow(); err != nil {
		return err
	}
	w.writes++
	if w.writes >= w.failOn {
		w.cb.RecordFailure()
		return errors.New("sink write error")
	}
	w.cb.RecordSuccess()
	return nil
}

func TestCircuit_SinkProtection_OpensOnRepeatedFailure(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 3, OpenDuration: 50 * time.Millisecond}
	cb := NewCircuitBreaker(cfg)
	sink := &wrappedSink{cb: cb, failOn: 1}

	var openSeen bool
	for i := 0; i < 10; i++ {
		err := sink.Write(Message{})
		if errors.Is(err, ErrCircuitOpen) {
			openSeen = true
			break
		}
	}
	if !openSeen {
		t.Fatal("circuit breaker never opened during repeated sink failures")
	}
}

func TestCircuit_SinkProtection_RecoveryAfterCooldown(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 1, OpenDuration: 60 * time.Millisecond}
	cb := NewCircuitBreaker(cfg)
	sink := &wrappedSink{cb: cb, failOn: 1}

	_ = sink.Write(Message{}) // triggers failure + opens circuit

	if err := sink.Write(Message{}); !errors.Is(err, ErrCircuitOpen) {
		t.Fatal("expected circuit to be open immediately after failure")
	}

	time.Sleep(80 * time.Millisecond)

	// half-open: next call should be allowed
	sink.failOn = 999 // no more failures
	if err := sink.Write(Message{}); err != nil {
		t.Fatalf("expected recovery after cooldown, got %v", err)
	}
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed after successful recovery, got %v", cb.State())
	}
}
