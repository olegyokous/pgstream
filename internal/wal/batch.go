package wal

import (
	"context"
	"time"
)

// BatchConfig controls how messages are grouped before flushing.
type BatchConfig struct {
	MaxSize  int
	MaxDelay time.Duration
}

// DefaultBatchConfig returns sensible defaults.
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		MaxSize:  100,
		MaxDelay: 500 * time.Millisecond,
	}
}

// Batcher accumulates WAL messages and flushes them in batches.
type Batcher struct {
	cfg    BatchConfig
	buf    []*Message
	flush  func([]*Message) error
}

// NewBatcher creates a Batcher that calls flush when a batch is ready.
func NewBatcher(cfg BatchConfig, flush func([]*Message) error) *Batcher {
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = DefaultBatchConfig().MaxSize
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = DefaultBatchConfig().MaxDelay
	}
	return &Batcher{
		cfg:   cfg,
		buf:   make([]*Message, 0, cfg.MaxSize),
		flush: flush,
	}
}

// Add appends a message to the current batch, flushing if the batch is full.
func (b *Batcher) Add(msg *Message) error {
	b.buf = append(b.buf, msg)
	if len(b.buf) >= b.cfg.MaxSize {
		return b.Flush()
	}
	return nil
}

// Flush sends all buffered messages to the flush callback and resets the buffer.
func (b *Batcher) Flush() error {
	if len(b.buf) == 0 {
		return nil
	}
	batch := make([]*Message, len(b.buf))
	copy(batch, b.buf)
	b.buf = b.buf[:0]
	return b.flush(batch)
}

// Run starts a ticker-based flush loop that drains the buffer every MaxDelay.
func (b *Batcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(b.cfg.MaxDelay)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = b.Flush()
			return ctx.Err()
		case <-ticker.C:
			if err := b.Flush(); err != nil {
				return err
			}
		}
	}
}

// Len returns the number of messages currently buffered.
func (b *Batcher) Len() int { return len(b.buf) }
