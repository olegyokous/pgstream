package wal

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// DLQConfig holds configuration for the dead-letter queue.
type DLQConfig struct {
	// MaxSize is the maximum number of messages to retain.
	MaxSize int
	// TTL is how long a dead-lettered message is retained before eviction.
	TTL time.Duration
}

// DefaultDLQConfig returns sensible defaults.
func DefaultDLQConfig() DLQConfig {
	return DLQConfig{
		MaxSize: 1000,
		TTL:     30 * time.Minute,
	}
}

// DLQEntry wraps a message that could not be processed.
type DLQEntry struct {
	Message  *Message
	Reason   string
	DeadAt   time.Time
}

// DeadLetterQueue stores messages that failed processing for later inspection
// or replay.
type DeadLetterQueue struct {
	mu      sync.Mutex
	entries []DLQEntry
	cfg     DLQConfig
	clock   func() time.Time
}

// NewDeadLetterQueue creates a DLQ with the given config.
func NewDeadLetterQueue(cfg DLQConfig) *DeadLetterQueue {
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = DefaultDLQConfig().MaxSize
	}
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultDLQConfig().TTL
	}
	return &DeadLetterQueue{cfg: cfg, clock: time.Now}
}

// Enqueue adds a failed message to the DLQ. If the queue is full the oldest
// entry is evicted to make room.
func (d *DeadLetterQueue) Enqueue(msg *Message, reason string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.evictExpired()
	if len(d.entries) >= d.cfg.MaxSize {
		d.entries = d.entries[1:] // drop oldest
	}
	d.entries = append(d.entries, DLQEntry{
		Message: msg,
		Reason:  reason,
		DeadAt:  d.clock(),
	})
}

// Drain returns and removes all current entries from the DLQ.
func (d *DeadLetterQueue) Drain() []DLQEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.evictExpired()
	out := make([]DLQEntry, len(d.entries))
	copy(out, d.entries)
	d.entries = d.entries[:0]
	return out
}

// Len returns the current number of entries in the DLQ.
func (d *DeadLetterQueue) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.evictExpired()
	return len(d.entries)
}

// Replay calls fn for every entry in the DLQ. Entries for which fn returns
// nil are removed; entries that return an error are kept.
func (d *DeadLetterQueue) Replay(ctx context.Context, fn func(*Message) error) error {
	d.mu.Lock()
	copied := make([]DLQEntry, len(d.entries))
	copy(copied, d.entries)
	d.mu.Unlock()

	var kept []DLQEntry
	var lastErr error
	for _, e := range copied {
		if ctx.Err() != nil {
			return fmt.Errorf("dlq replay cancelled: %w", ctx.Err())
		}
		if err := fn(e.Message); err != nil {
			e.Reason = err.Error()
			kept = append(kept, e)
			lastErr = err
		}
	}
	d.mu.Lock()
	d.entries = kept
	d.mu.Unlock()
	return lastErr
}

// evictExpired removes entries older than TTL. Caller must hold mu.
func (d *DeadLetterQueue) evictExpired() {
	cutoff := d.clock().Add(-d.cfg.TTL)
	i := 0
	for i < len(d.entries) && d.entries[i].DeadAt.Before(cutoff) {
		i++
	}
	d.entries = d.entries[i:]
}
