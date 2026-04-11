package wal

import (
	"context"
	"fmt"
)

// Semaphore is a counting semaphore that limits concurrent access to a resource.
type Semaphore struct {
	ch chan struct{}
}

// NewSemaphore creates a Semaphore with the given concurrency limit.
// It returns an error if n is less than 1.
func NewSemaphore(n int) (*Semaphore, error) {
	if n < 1 {
		return nil, fmt.Errorf("semaphore: concurrency limit must be at least 1, got %d", n)
	}
	ch := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		ch <- struct{}{}
	}
	return &Semaphore{ch: ch}, nil
}

// Acquire blocks until a slot is available or ctx is done.
// Returns ctx.Err() if the context is cancelled before a slot is acquired.
func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case <-s.ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryAcquire attempts to acquire a slot without blocking.
// Returns true if successful, false if no slots are available.
func (s *Semaphore) TryAcquire() bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

// Release returns a slot to the semaphore.
// It panics if called more times than Acquire.
func (s *Semaphore) Release() {
	select {
	case s.ch <- struct{}{}:
	default:
		panic("semaphore: Release called without matching Acquire")
	}
}

// Available returns the number of currently available slots.
func (s *Semaphore) Available() int {
	return len(s.ch)
}

// Capacity returns the maximum number of concurrent holders.
func (s *Semaphore) Capacity() int {
	return cap(s.ch)
}
