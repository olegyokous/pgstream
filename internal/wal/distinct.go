package wal

import "sync"

// Distincter drops messages whose key (table+action+primary column value)
// has already been seen in the current session. Unlike Deduplicator it has no
// TTL – once a key is seen it is suppressed forever until Reset is called.
type Distincter struct {
	mu      sync.Mutex
	seen    map[string]struct{}
	keyCol  string
	table   string // empty = match all tables
}

// NewDistincter creates a Distincter that tracks uniqueness by the given
// column. If table is non-empty only messages for that table are filtered;
// others pass through unchanged.
func NewDistincter(keyCol, table string) (*Distincter, error) {
	if keyCol == "" {
		return nil, errorf("distincter: keyCol must not be empty")
	}
	return &Distincter{
		seen:   make(map[string]struct{}),
		keyCol: keyCol,
		table:  table,
	}, nil
}

// Apply returns (msg, true) when the message is novel and should be forwarded,
// or (nil, false) when it is a duplicate and should be dropped.
func (d *Distincter) Apply(msg *Message) (*Message, bool) {
	if msg == nil {
		return nil, false
	}
	if d.table != "" && msg.Table != d.table {
		return msg, true
	}

	val := ""
	for _, col := range msg.Columns {
		if col.Name == d.keyCol {
			if col.Value != nil {
				val = col.Value.(string)
			}
			break
		}
	}

	key := msg.Table + "|" + msg.Action + "|" + val

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.seen[key]; exists {
		return nil, false
	}
	d.seen[key] = struct{}{}
	return msg, true
}

// Reset clears all previously seen keys.
func (d *Distincter) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen = make(map[string]struct{})
}

// Len returns the number of unique keys currently tracked.
func (d *Distincter) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
