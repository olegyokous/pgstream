package wal

import "fmt"

// RejectRule defines a condition under which a message should be rejected with an error.
type RejectRule struct {
	Table  string
	Action string
	Reason string
}

// Rejecter returns an error for messages matching configured rules.
type Rejecter struct {
	rules []RejectRule
}

// NewRejecter creates a Rejecter with the given rules.
func NewRejecter(rules []RejectRule) (*Rejecter, error) {
	if len(rules) == 0 {
		return nil, fmt.Errorf("rejecter: at least one rule is required")
	}
	for i, r := range rules {
		if r.Reason == "" {
			return nil, fmt.Errorf("rejecter: rule %d missing reason", i)
		}
	}
	return &Rejecter{rules: rules}, nil
}

// Apply returns an error if the message matches any rule, otherwise returns nil.
func (r *Rejecter) Apply(msg *Message) error {
	if msg == nil {
		return nil
	}
	for _, rule := range r.rules {
		if rule.Table != "" && rule.Table != msg.Table {
			continue
		}
		if rule.Action != "" && rule.Action != msg.Action {
			continue
		}
		return fmt.Errorf("rejecter: message rejected: %s", rule.Reason)
	}
	return nil
}
