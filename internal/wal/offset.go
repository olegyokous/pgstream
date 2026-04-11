package wal

import (
	"fmt"
	"sync"
)

// OffsetTracker tracks the latest committed and pending LSN offsets
// for a replication slot, enabling safe progress reporting.
type OffsetTracker struct {
	mu        sync.Mutex
	committed uint64
	pending   uint64
}

// NewOffsetTracker returns an OffsetTracker starting at the given LSN.
func NewOffsetTracker(start uint64) *OffsetTracker {
	return &OffsetTracker{
		committed: start,
		pending:   start,
	}
}

// Advance marks lsn as the latest pending offset if it is greater than
// the current pending value. It returns an error if lsn is behind committed.
func (o *OffsetTracker) Advance(lsn uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if lsn < o.committed {
		return fmt.Errorf("offset: lsn %d is behind committed %d", lsn, o.committed)
	}
	if lsn > o.pending {
		o.pending = lsn
	}
	return nil
}

// Commit moves the committed cursor up to the current pending value.
// It returns the new committed LSN.
func (o *OffsetTracker) Commit() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.committed = o.pending
	return o.committed
}

// Committed returns the last committed LSN.
func (o *OffsetTracker) Committed() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.committed
}

// Pending returns the latest pending (uncommitted) LSN.
func (o *OffsetTracker) Pending() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.pending
}

// Lag returns the number of LSN units between committed and pending.
func (o *OffsetTracker) Lag() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.pending > o.committed {
		return o.pending - o.committed
	}
	return 0
}
