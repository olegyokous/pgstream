package wal

import "strings"

// CoalesceConfig controls how column values are merged when multiple updates
// arrive for the same row key within a single processing pass.
type CoalesceConfig struct {
	// KeyColumn is the column used to identify the same logical row.
	KeyColumn string
	// Tables restricts coalescing to the named tables; empty means all tables.
	Tables []string
}

// Coalescer merges consecutive messages that share the same key column value
// and table, keeping only the most recent field values (last-write-wins).
type Coalescer struct {
	cfg    CoalesceConfig
	tables map[string]struct{}
}

// NewCoalescer returns a Coalescer configured with cfg.
// An error is returned when KeyColumn is empty.
func NewCoalescer(cfg CoalesceConfig) (*Coalescer, error) {
	if strings.TrimSpace(cfg.KeyColumn) == "" {
		return nil, errorf("coalescer: KeyColumn must not be empty")
	}
	tables := make(map[string]struct{}, len(cfg.Tables))
	for _, t := range cfg.Tables {
		tables[strings.ToLower(t)] = struct{}{}
	}
	return &Coalescer{cfg: cfg, tables: tables}, nil
}

// applies returns true when the coalescer should act on msg.
func (c *Coalescer) applies(msg *Message) bool {
	if msg == nil {
		return false
	}
	if len(c.tables) == 0 {
		return true
	}
	_, ok := c.tables[strings.ToLower(msg.Table)]
	return ok
}

// Merge combines src into dst using last-write-wins for every column.
// dst is modified in place and returned. If either message is nil or the
// tables differ, dst is returned unchanged.
func (c *Coalescer) Merge(dst, src *Message) *Message {
	if dst == nil {
		return src
	}
	if src == nil || dst.Table != src.Table {
		return dst
	}
	if !c.applies(dst) {
		return dst
	}
	merged := make(map[string]any, len(dst.Columns))
	for k, v := range dst.Columns {
		merged[k] = v
	}
	for k, v := range src.Columns {
		merged[k] = v
	}
	dst.Columns = merged
	dst.LSN = src.LSN
	return dst
}

// CoalesceSlice reduces msgs to a deduplicated slice where only the last
// message for each (table, key) pair survives. Order of first appearance is
// preserved.
func (c *Coalescer) CoalesceSlice(msgs []*Message) []*Message {
	type rowKey struct{ table, key string }
	index := make(map[rowKey]int, len(msgs))
	out := make([]*Message, 0, len(msgs))
	for _, m := range msgs {
		if m == nil {
			continue
		}
		if !c.applies(m) {
			out = append(out, m)
			continue
		}
		keyVal := ""
		if m.Columns != nil {
			if v, ok := m.Columns[c.cfg.KeyColumn]; ok && v != nil {
				keyVal = strings.ToLower(fmt.Sprint(v))
			}
		}
		rk := rowKey{table: m.Table, key: keyVal}
		if pos, seen := index[rk]; seen {
			out[pos] = c.Merge(out[pos], m)
		} else {
			index[rk] = len(out)
			out = append(out, m)
		}
	}
	return out
}
