package wal

import "strings"

// EnrichRule defines a static key/value pair to inject into a message's metadata.
type EnrichRule struct {
	Key   string
	Value string
}

// Enricher appends static or derived metadata fields to WAL messages.
type Enricher struct {
	rules  []EnrichRule
	tables map[string]struct{}
}

// NewEnricher creates an Enricher that applies rules to messages matching the
// given tables. An empty tables slice matches all tables.
func NewEnricher(rules []EnrichRule, tables []string) (*Enricher, error) {
	if len(rules) == 0 {
		return nil, errorf("enricher: at least one rule is required")
	}
	tm := make(map[string]struct{}, len(tables))
	for _, t := range tables {
		tm[strings.ToLower(t)] = struct{}{}
	}
	return &Enricher{rules: rules, tables: tm}, nil
}

// Apply injects metadata fields into msg and returns the modified message.
// If msg is nil it is returned unchanged.
func (e *Enricher) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	if len(e.tables) > 0 {
		if _, ok := e.tables[strings.ToLower(msg.Table)]; !ok {
			return msg
		}
	}
	if msg.Meta == nil {
		msg.Meta = make(map[string]string)
	}
	for _, r := range e.rules {
		msg.Meta[r.Key] = r.Value
	}
	return msg
}

// errorf is a thin wrapper so the package stays self-contained.
func errorf(s string) error {
	return &enrichError{msg: s}
}

type enrichError struct{ msg string }

func (e *enrichError) Error() string { return e.msg }
