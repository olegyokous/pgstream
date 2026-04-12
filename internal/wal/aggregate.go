package wal

import (
	"fmt"
	"sync"
)

// AggregateConfig holds configuration for the Aggregator.
type AggregateConfig struct {
	// GroupBy determines the field used to group messages: "table" or "action".
	GroupBy string
}

// DefaultAggregateConfig returns an AggregateConfig with sensible defaults.
func DefaultAggregateConfig() AggregateConfig {
	return AggregateConfig{
		GroupBy: "table",
	}
}

// Aggregator groups WAL messages by a key and counts occurrences.
type Aggregator struct {
	cfg    AggregateConfig
	mu     sync.Mutex
	counts map[string]int
}

// NewAggregator creates a new Aggregator with the given config.
// Returns an error if GroupBy is not "table" or "action".
func NewAggregator(cfg AggregateConfig) (*Aggregator, error) {
	if cfg.GroupBy != "table" && cfg.GroupBy != "action" {
		return nil, fmt.Errorf("aggregate: unsupported GroupBy %q; must be \"table\" or \"action\"", cfg.GroupBy)
	}
	return &Aggregator{
		cfg:    cfg,
		counts: make(map[string]int),
	}, nil
}

// Record increments the counter for the key derived from msg.
// Nil messages are silently ignored.
func (a *Aggregator) Record(msg *Message) {
	if msg == nil {
		return
	}
	key := a.keyFor(msg)
	a.mu.Lock()
	a.counts[key]++
	a.mu.Unlock()
}

// Snapshot returns a copy of the current counts map.
func (a *Aggregator) Snapshot() map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[string]int, len(a.counts))
	for k, v := range a.counts {
		out[k] = v
	}
	return out
}

// Reset clears all accumulated counts.
func (a *Aggregator) Reset() {
	a.mu.Lock()
	a.counts = make(map[string]int)
	a.mu.Unlock()
}

func (a *Aggregator) keyFor(msg *Message) string {
	if a.cfg.GroupBy == "action" {
		return msg.Action
	}
	return msg.Table
}
