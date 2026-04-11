package wal

import (
	"context"
	"testing"
	"time"
)

func TestNewSemaphore_InvalidLimit(t *testing.T) {
	_, err := NewSemaphore(0)
	if err == nil {
		t.Fatal("expected error for limit=0, got nil")
	}
}

func TestNewSemaphore_ValidLimit(t *testing.T) {
	sem, err := NewSemaphore(3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sem.Capacity() != 3 {
		t.Errorf("expected capacity 3, got %d", sem.Capacity())
	}
	if sem.Available() != 3 {
		t.Errorf("expected 3 available slots, got %d", sem.Available())
	}
}

func TestSemaphore_AcquireAndRelease(t *testing.T) {
	sem, _ := NewSemaphore(2)

	ctx := context.Background()
	if err := sem.Acquire(ctx); err != nil {
		t.Fatalf("first Acquire failed: %v", err)
	}
	if sem.Available() != 1 {
		t.Errorf("expected 1 available after acquire, got %d", sem.Available())
	}

	if err := sem.Acquire(ctx); err != nil {
		t.Fatalf("second Acquire failed: %v", err)
	}
	if sem.Available() != 0 {
		t.Errorf("expected 0 available after two acquires, got %d", sem.Available())
	}

	sem.Release()
	if sem.Available() != 1 {
		t.Errorf("expected 1 available after release, got %d", sem.Available())
	}
}

func TestSemaphore_TryAcquire(t *testing.T) {
	sem, _ := NewSemaphore(1)

	if !sem.TryAcquire() {
		t.Fatal("expected TryAcquire to succeed on empty semaphore")
	}
	if sem.TryAcquire() {
		t.Fatal("expected TryAcquire to fail when semaphore is full")
	}
	sem.Release()
	if !sem.TryAcquire() {
		t.Fatal("expected TryAcquire to succeed after release")
	}
}

func TestSemaphore_AcquireRespectsContextCancellation(t *testing.T) {
	sem, _ := NewSemaphore(1)
	_ = sem.TryAcquire() // exhaust the slot

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := sem.Acquire(ctx)
	if err == nil {
		t.Fatal("expected error when context is cancelled, got nil")
	}
}

func TestSemaphore_ReleasePanicsOnOverflow(t *testing.T) {
	sem, _ := NewSemaphore(1)
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on over-release, got none")
		}
	}()
	sem.Release() // never acquired — should panic
}
