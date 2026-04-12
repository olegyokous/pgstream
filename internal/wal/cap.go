package wal

import "fmt"

// Capper limits the number of messages processed in a single run.
// Once the cap is reached, subsequent messages are dropped until Reset is called.
type Capper struct {
	max     int
	count   int
	table   string
	action  string
}

// NewCapper returns a Capper that allows at most max messages through.
// Optionally scope by table and/or action; empty string means match all.
func NewCapper(max int, table, action string) (*Capper, error) {
	if max <= 0 {
		return nil, fmt.Errorf("cap: max must be greater than zero, got %d", max)
	}
	return &Capper{max: max, table: table, action: action}, nil
}

// Allow returns true if the message should be forwarded, false if it should be dropped.
// A nil message is always dropped.
func (c *Capper) Allow(msg *Message) bool {
	if msg == nil {
		return false
	}
	if c.table != "" && msg.Table != c.table {
		return true // out of scope — pass through
	}
	if c.action != "" && string(msg.Action) != c.action {
		return true // out of scope — pass through
	}
	if c.count >= c.max {
		return false
	}
	c.count++
	return true
}

// Reset clears the internal counter so the cap applies fresh.
func (c *Capper) Reset() {
	c.count = 0
}

// Remaining returns how many more messages are allowed before the cap is hit.
func (c *Capper) Remaining() int {
	rem := c.max - c.count
	if rem < 0 {
		return 0
	}
	return rem
}
