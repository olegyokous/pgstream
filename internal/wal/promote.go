package wal

import "strings"

// PromoterRule defines a condition and the meta key/value to set when matched.
type PromoterRule struct {
	Table  string // empty matches all tables
	Action string // empty matches all actions
	Column string // column whose value is promoted into Meta
	MetaKey string // destination key in Meta map
}

// Promoter lifts a column value from a message's Columns into its Meta map.
type Promoter struct {
	rules []PromoterRule
}

// NewPromoter returns a Promoter that applies the given rules.
// At least one rule must be provided.
func NewPromoter(rules []PromoterRule) (*Promoter, error) {
	if len(rules) == 0 {
		return nil, errorf("promoter requires at least one rule")
	}
	for i, r := range rules {
		if r.Column == "" {
			return nil, errorf("promoter rule %d: column must not be empty", i)
		}
		if r.MetaKey == "" {
			return nil, errorf("promoter rule %d: meta_key must not be empty", i)
		}
	}
	return &Promoter{rules: rules}, nil
}

// Apply promotes column values into the message Meta according to matching rules.
// The original message is returned unchanged if no rule matches.
func (p *Promoter) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	for _, r := range p.rules {
		if !ruleMatchesPromote(r, msg) {
			continue
		}
		for _, col := range msg.Columns {
			if !strings.EqualFold(col.Name, r.Column) {
				continue
			}
			if msg.Meta == nil {
				msg.Meta = make(map[string]string)
			}
			if col.Value != nil {
				msg.Meta[r.MetaKey] = col.Value.(string)
			}
		}
	}
	return msg
}

func ruleMatchesPromote(r PromoterRule, msg *Message) bool {
	if r.Table != "" && !strings.EqualFold(msg.Table, r.Table) {
		return false
	}
	if r.Action != "" && !strings.EqualFold(string(msg.Action), r.Action) {
		return false
	}
	return true
}
