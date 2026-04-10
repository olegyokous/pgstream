package wal

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_ExecutesAllTasks(t *testing.T) {
	p := NewPool(PoolConfig{Workers: 2, QueueDepth: 16})
	var count int64
	for i := 0; i < 10; i++ {
		if err := p.Submit(context.Background(), func(ctx context.Context) error {
			atomic.AddInt64(&count, 1)
			return nil
		}); err != nil {
			t.Fatalf("submit failed: %v", err)
		}
	}
	p.Close()
	if count != 10 {
		t.Errorf("expected 10 tasks executed, got %d", count)
	}
}

func TestPool_ErrorsForwardedToChannel(t *testing.T) {
	p := NewPool(PoolConfig{Workers: 1, QueueDepth: 8})
	sentinel := errors.New("task error")
	_ = p.Submit(context.Background(), func(ctx context.Context) error {
		return sentinel
	})
	p.Close()
	var got error
	for e := range p.Errors {
		got = e
	}
	if !errors.Is(got, sentinel) {
		t.Errorf("expected sentinel error, got %v", got)
	}
}

func TestPool_SubmitAfterCloseReturnsError(t *testing.T) {
	p := NewPool(DefaultPoolConfig())
	p.Close()
	err := p.Submit(context.Background(), func(ctx context.Context) error { return nil })
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}
}

func TestPool_SubmitRespectsContextCancellation(t *testing.T) {
	// Fill the queue so Submit blocks.
	p := NewPool(PoolConfig{Workers: 1, QueueDepth: 1})
	// Occupy the single slot.
	_ = p.Submit(context.Background(), func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := p.Submit(ctx, func(ctx context.Context) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
	p.Close()
}

func TestPool_DefaultConfigApplied(t *testing.T) {
	p := NewPool(PoolConfig{})
	if p.cfg.Workers != 4 {
		t.Errorf("expected 4 workers, got %d", p.cfg.Workers)
	}
	if p.cfg.QueueDepth != 64 {
		t.Errorf("expected queue depth 64, got %d", p.cfg.QueueDepth)
	}
	p.Close()
}
