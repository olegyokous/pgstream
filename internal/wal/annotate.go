package wal

import "strings"

// AnnotateRule describes a single annotation: when a message matches the
// given table (empty = all tables) and action (empty = all actions) the
// key/value pair is written into the message Meta map.
type AnnotateRule struct {
	Table  string
	Action string
	Key    string
	Value  string
}

// Annotator stamps key/value metadata onto messages that match a set of rules.
type Annotator struct {
	rules []AnnotateRule
}

// NewAnnotator returns an Annotator that applies the supplied rules in order.
// At least one rule must be provided.
func NewAnnotator(rules []AnnotateRule) (*Annotator, error) {
	if len(rules) == 0 {
		return nil, errorf("annotator: at least one rule is required")
	}
	for i, r := range rules {
		if r.Key == "" {
			return nil, errorf("annotator: rule %d has an empty key", i)
		}
	}
	return &Annotator{rules: rules}, nil
}

// Apply stamps matching annotations onto msg and returns it unchanged if no
// rule matches. A nil message is returned as-is.
func (a *Annotator) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	for _, r := range a.rules {
		if !ruleMatchesAnnotate(r, msg) {
			continue
		}
		if msg.Meta == nil {
			msg.Meta = make(map[string]string)
		}
		msg.Meta[r.Key] = r.Value
	}
	return msg
}

func ruleMatchesAnnotate(r AnnotateRule, msg *Message) bool {
	if r.Table != "" && !strings.EqualFold(msg.Table, r.Table) {
		return false
	}
	if r.Action != "" && !strings.EqualFold(string(msg.Action), r.Action) {
		return false
	}
	return true
}
