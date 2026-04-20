package wal

import (
	"context"
	"sync"
	"time"
)

// FlushFunc is called with the highest confirmed LSN when a checkpoint flush occurs.
type FlushFunc func(ctx context.Context, lsn uint64) error

// CheckpointConfig holds configuration for the checkpoint manager.
type CheckpointConfig struct {
	FlushInterval time.Duration
	FlushFn       FlushFunc
}

// DefaultCheckpointManagerConfig returns sensible defaults.
func DefaultCheckpointManagerConfig(fn FlushFunc) CheckpointConfig {
	return CheckpointConfig{
		FlushInterval: 5 * time.Second,
		FlushFn:       fn,
	}
}

// CheckpointManager tracks WAL LSN positions and periodically flushes
// the highest confirmed position to the upstream replication slot.
type CheckpointManager struct {
	mu       sync.Mutex
	pending  uint64
	committed uint64
	cfg      CheckpointConfig
}

// NewCheckpointManager constructs a CheckpointManager with the given config.
func NewCheckpointManager(cfg CheckpointConfig) *CheckpointManager {
	return &CheckpointManager{cfg: cfg}
}

// Track records a new LSN as pending, advancing only if it is greater than
// the current pending position.
func (c *CheckpointManager) Track(lsn uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if lsn > c.pending {
		c.pending = lsn
	}
}

// Committed returns the last successfully flushed LSN.
func (c *CheckpointManager) Committed() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.committed
}

// flush calls the configured FlushFn if there is a new pending LSN.
func (c *CheckpointManager) flush(ctx context.Context) error {
	c.mu.Lock()
	lsn := c.pending
	c.mu.Unlock()

	if lsn == 0 || lsn == c.committed {
		return nil
	}
	if err := c.cfg.FlushFn(ctx, lsn); err != nil {
		return err
	}
	c.mu.Lock()
	c.committed = lsn
	c.mu.Unlock()
	return nil
}

// Run starts the periodic flush loop. It blocks until ctx is cancelled.
func (c *CheckpointManager) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = c.flush(context.Background())
			return ctx.Err()
		case <-ticker.C:
			if err := c.flush(ctx); err != nil {
				return err
			}
		}
	}
}
