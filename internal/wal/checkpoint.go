package wal

import (
	"context"
	"log"
	"sync"
	"time"
)

// CheckpointConfig holds configuration for the checkpoint manager.
type CheckpointConfig struct {
	// FlushInterval is how often accumulated LSNs are flushed to the server.
	FlushInterval time.Duration
}

// DefaultCheckpointConfig returns a sensible default checkpoint configuration.
func DefaultCheckpointConfig() CheckpointConfig {
	return CheckpointConfig{
		FlushInterval: 5 * time.Second,
	}
}

// FlushFunc is called with the latest LSN to acknowledge to the server.
type FlushFunc func(ctx context.Context, lsn uint64) error

// CheckpointManager tracks the latest processed LSN and periodically
// acknowledges it back to PostgreSQL so the WAL can be reclaimed.
type CheckpointManager struct {
	cfg     CheckpointConfig
	flushFn FlushFunc

	mu     sync.Mutex
	latest uint64
}

// NewCheckpointManager creates a new CheckpointManager.
func NewCheckpointManager(cfg CheckpointConfig, fn FlushFunc) *CheckpointManager {
	return &CheckpointManager{
		cfg:     cfg,
		flushFn: fn,
	}
}

// Track records a processed LSN. Only advances the cursor forward.
func (c *CheckpointManager) Track(lsn uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if lsn > c.latest {
		c.latest = lsn
	}
}

// Latest returns the most recently tracked LSN.
func (c *CheckpointManager) Latest() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.latest
}

// Run starts the periodic flush loop. It blocks until ctx is cancelled.
func (c *CheckpointManager) Run(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lsn := c.Latest()
			if lsn == 0 {
				continue
			}
			ifn(ctx, lsn); err != nil {
				log.Printf("checkpoint flush error: %v", err)
			}\t}
}
