package wal

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewHedger_DefaultConfigApplied(t *testing.T) {
	h := NewHedger(HedgeConfig{})
	if h.cfg.MaxHedges < 2 {
		t.Fatalf("expected MaxHedges >= 2, got %d", h.cfg.MaxHedges)
	}
	if h.cfg.Delay <= 0 {
		t.Fatalf("expected positive Delay, got %v", h.cfg.Delay)
	}
}

func TestHedger_FirstAttemptSucceeds(t *testing.T) {
	h := NewHedger(HedgeConfig{Delay: 10 * time.Millisecond, MaxHedges: 2})
	calls := int32(0)
	got, err := h.Do(context.Background(), func(ctx context.Context) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return []byte("ok"), nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != "ok" {
		t.Fatalf("expected 'ok', got %q", got)
	}
}

func TestHedger_HedgeWinsWhenPrimaryIsSlow(t *testing.T) {
	h := NewHedger(HedgeConfig{Delay: 20 * time.Millisecond, MaxHedges: 2})
	calls := int32(0)
	got, err := h.Do(context.Background(), func(ctx context.Context) ([]byte, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return []byte("slow"), nil
			}
		}
		return []byte("hedge"), nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != "hedge" {
		t.Fatalf("expected 'hedge', got %q", got)
	}
}

func TestHedger_AllAttemptsFailReturnsError(t *testing.T) {
	h := NewHedger(HedgeConfig{Delay: 5 * time.Millisecond, MaxHedges: 2})
	sentinel := errors.New("boom")
	_, err := h.Do(context.Background(), func(ctx context.Context) ([]byte, error) {
		return nil, sentinel
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestHedger_ContextCancellationReturnsError(t *testing.T) {
	h := NewHedger(HedgeConfig{Delay: 10 * time.Millisecond, MaxHedges: 2})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := h.Do(ctx, func(ctx context.Context) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
