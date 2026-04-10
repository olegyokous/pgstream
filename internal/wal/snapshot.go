package wal

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// Snapshot captures a point-in-time view of WAL message grouped by
// table and action, useful for diagnostics and reporting.
type Snapshot struct {
	CapturedAt time.Time
	Counts     map[string]int64 // key: "table:action"
}

// SnapshotCollector accumulates WAL messages and can produce snapshots.
type SnapshotCollector struct {
	mu     sync.Mutex
	counts map[string]int64
}

// NewSnapshotCollector returns an initialised SnapshotCollector.
func NewSnapshotCollector() *SnapshotCollector {
	return &SnapshotCollector{
		counts: make(map[string]int64),
	}
}

// Record increments the counter for the given table/action pair.
func (sc *SnapshotCollector) Record(table, action string) {
	key := fmt.Sprintf("%s:%s", table, action)
	sc.mu.Lock()
	sc.counts[key]++
	sc.mu.Unlock()
}

// Snapshot returns an immutable point-in-time copy of the current counts.
func (sc *SnapshotCollector) Snapshot() Snapshot {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	copy := make(map[string]int64, len(sc.counts))
	for k, v := range sc.counts {
		copy[k] = v
	}
	return Snapshot{
		CapturedAt: time.Now(),
		Counts:     copy,
	}
}

// Reset clears all accumulated counts.
func (sc *SnapshotCollector) Reset() {
	sc.mu.Lock()
	sc.counts = make(map[string]int64)
	sc.mu.Unlock()
}

// Total returns the sum of all recorded events.
func (sc *SnapshotCollector) Total() int64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	var total int64
	for _, v := range sc.counts {
		total += v
	}
	return total
}

// Summary returns a human-readable string of all non-zero counts.
func (s Snapshot) Summary() string {
	if len(s.Counts) == 0 {
		return "no events recorded"
	}
	parts := make([]string, 0, len(s.Counts))
	for k, v := range s.Counts {
		parts = append(parts, fmt.Sprintf("%s=%d", k, v))
	}
	return strings.Join(parts, " ")
}
