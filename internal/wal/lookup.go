package wal

import (
	"fmt"
	"strings"
)

// LookupRule maps a column value to a replacement using a static table.
type LookupRule struct {
	Table  string
	Column string
	Map    map[string]string
	Fallback string // empty means keep original
}

// Lookuper replaces column values based on a lookup table.
type Lookuper struct {
	rules []LookupRule
}

// NewLookuper returns a Lookuper with the given rules.
// At least one rule must be provided.
func NewLookuper(rules []LookupRule) (*Lookuper, error) {
	if len(rules) == 0 {
		return nil, fmt.Errorf("lookuper: at least one rule is required")
	}
	for i, r := range rules {
		if r.Column == "" {
			return nil, fmt.Errorf("lookuper: rule %d missing column", i)
		}
		if len(r.Map) == 0 {
			return nil, fmt.Errorf("lookuper: rule %d has empty map", i)
		}
	}
	return &Lookuper{rules: rules}, nil
}

// Apply translates column values in msg according to the lookup rules.
// Returns nil if msg is nil.
func (l *Lookuper) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	for _, rule := range l.rules {
		if rule.Table != "" && !strings.EqualFold(msg.Table, rule.Table) {
			continue
		}
		for i, col := range msg.Columns {
			if !strings.EqualFold(col.Name, rule.Column) {
				continue
			}
			raw, _ := col.Value.(string)
			if mapped, ok := rule.Map[raw]; ok {
				msg.Columns[i].Value = mapped
			} else if rule.Fallback != "" {
				msg.Columns[i].Value = rule.Fallback
			}
		}
	}
	return msg
}
