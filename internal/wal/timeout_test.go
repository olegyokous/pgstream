package wal

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTimeouter_DefaultConfigApplied(t *testing.T) {
	tm := NewTimeouter(TimeoutConfig{})
	if tm.Duration() != 5*time.Second {
		t.Fatalf("expected default 5s, got %s", tm.Duration())
	}
}

func TestTimeouter_SucceedsWhenFnCompletesInTime(t *testing.T) {
	tm := NewTimeouter(TimeoutConfig{Duration: 100 * time.Millisecond})
	err := tm.Do(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTimeouter_ReturnsErrorFromFn(t *testing.T) {
	sentinel := errors.New("fn error")
	tm := NewTimeouter(TimeoutConfig{Duration: 100 * time.Millisecond})
	err := tm.Do(context.Background(), func(ctx context.Context) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}

func TestTimeouter_TimesOutSlowOperation(t *testing.T) {
	tm := NewTimeouter(TimeoutConfig{Duration: 30 * time.Millisecond})
	err := tm.Do(context.Background(), func(ctx context.Context) error {
		select {
		case <-time.After(500 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestTimeouter_RespectsParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tm := NewTimeouter(TimeoutConfig{Duration: 5 * time.Second})
	err := tm.Do(ctx, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if err == nil {
		t.Fatal("expected error due to cancelled parent context")
	}
}
