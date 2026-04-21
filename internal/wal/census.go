package wal

import (
	"fmt"
	"sync"
)

// Census tracks per-table, per-action message counts across a stream.
// It is safe for concurrent use.
type Census struct {
	mu     sync.RWMutex
	counts map[string]map[string]int64
}

// NewCensus returns an initialised Census.
func NewCensus() *Census {
	return &Census{
		counts: make(map[string]map[string]int64),
	}
}

// Record increments the counter for the given table and action.
func (c *Census) Record(msg *Message) {
	if msg == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.counts[msg.Table]; !ok {
		c.counts[msg.Table] = make(map[string]int64)
	}
	c.counts[msg.Table][msg.Action]++
}

// Count returns the number of messages recorded for the given table and action.
func (c *Census) Count(table, action string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if actions, ok := c.counts[table]; ok {
		return actions[action]
	}
	return 0
}

// Tables returns all table names that have been recorded.
func (c *Census) Tables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tables := make([]string, 0, len(c.counts))
	for t := range c.counts {
		tables = append(tables, t)
	}
	return tables
}

// Reset clears all recorded counts.
func (c *Census) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts = make(map[string]map[string]int64)
}

// Summary returns a human-readable summary of all counts.
func (c *Census) Summary() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.counts) == 0 {
		return "census: no records"
	}
	out := "census:\n"
	for table, actions := range c.counts {
		for action, n := range actions {
			out += fmt.Sprintf("  %s/%s: %d\n", table, action, n)
		}
	}
	return out
}
