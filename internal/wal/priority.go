package wal

import (
	"errors"
	"sort"
)

// Priority levels for WAL messages.
const (
	PriorityLow    = 0
	PriorityNormal = 1
	PriorityHigh   = 2
)

// PriorityRule assigns a priority level to messages matching a table and/or action.
type PriorityRule struct {
	Table    string
	Action   string
	Priority int
}

// Prioritizer assigns integer priority levels to messages based on configurable rules.
type Prioritizer struct {
	rules        []PriorityRule
	defaultLevel int
}

// NewPrioritizer creates a Prioritizer with the given rules and default priority level.
// Rules are evaluated in order; the first match wins.
func NewPrioritizer(defaultLevel int, rules []PriorityRule) (*Prioritizer, error) {
	if len(rules) == 0 {
		return nil, errors.New("prioritizer: at least one rule is required")
	}
	return &Prioritizer{rules: rules, defaultLevel: defaultLevel}, nil
}

// Assign returns the priority level for msg.
func (p *Prioritizer) Assign(msg *Message) int {
	if msg == nil {
		return p.defaultLevel
	}
	for _, r := range p.rules {
		tableMatch := r.Table == "" || r.Table == msg.Table
		actionMatch := r.Action == "" || r.Action == msg.Action
		if tableMatch && actionMatch {
			return r.Priority
		}
	}
	return p.defaultLevel
}

// Sort reorders msgs in-place from highest to lowest priority.
func (p *Prioritizer) Sort(msgs []*Message) {
	sort.SliceStable(msgs, func(i, j int) bool {
		return p.Assign(msgs[i]) > p.Assign(msgs[j])
	})
}
