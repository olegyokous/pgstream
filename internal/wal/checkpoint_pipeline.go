package wal

import (
	"context"
	"sync/atomic"
	"time"
)

// PipelineCheckpointer wraps CheckpointManager and exposes a helper that
// records the LSN carried by a Message so callers don't have to unpack it
// manually.
type PipelineCheckpointer struct {
	cm *CheckpointManager
}

// NewPipelineCheckpointer creates a PipelineCheckpointer backed by a
// CheckpointManager built from cfg. flush is called on every interval with
// the highest LSN seen since the last flush.
func NewPipelineCheckpointer(cfg CheckpointConfig, flush func(uint64) error) *PipelineCheckpointer {
	return &PipelineCheckpointer{cm: NewCheckpointManager(cfg, flush)}
}

// Record extracts the LSN from msg and forwards it to the underlying
// CheckpointManager. A nil message is a no-op.
func (p *PipelineCheckpointer) Record(msg *Message) {
	if msg == nil {
		return
	}
	p.cm.Track(msg.LSN)
}

// Run starts the periodic flush loop and blocks until ctx is cancelled.
func (p *PipelineCheckpointer) Run(ctx context.Context) {
	p.cm.Run(ctx)
}

// lsnHighWater is a convenience type used internally to track the highest
// LSN seen across concurrent goroutines without a mutex.
type lsnHighWater struct {
	v atomic.Uint64
}

func (h *lsnHighWater) update(lsn uint64) {
	for {
		old := h.v.Load()
		if lsn <= old {
			return
		}
		if h.v.CompareAndSwap(old, lsn) {
			return
		}
	}
}

func (h *lsnHighWater) load() uint64 { return h.v.Load() }

// RunAutoCheckpoint is a standalone helper that periodically calls flush with
// the highest LSN provided via the returned track function. It returns when
// ctx is cancelled.
func RunAutoCheckpoint(ctx context.Context, interval time.Duration, flush func(uint64) error) func(uint64) {
	hw := &lsnHighWater{}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if lsn := hw.load(); lsn !=	_ = flush(lsn)
				}
				return hw.update
}
