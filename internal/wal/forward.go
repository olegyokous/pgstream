package wal

import "fmt"

// Forwarder conditionally forwards messages to a downstream writer based on a
// predicate. Messages that do not match are passed through unchanged.
type Forwarder struct {
	predicate func(*Message) bool
	target    func(*Message) error
}

// ForwarderOption configures a Forwarder.
type ForwarderOption func(*Forwarder)

// WithForwardTarget sets the downstream writer invoked when the predicate matches.
func WithForwardTarget(fn func(*Message) error) ForwarderOption {
	return func(f *Forwarder) { f.target = fn }
}

// NewForwarder creates a Forwarder that calls target whenever predicate returns
// true. Both predicate and target must be non-nil.
func NewForwarder(predicate func(*Message) bool, opts ...ForwarderOption) (*Forwarder, error) {
	if predicate == nil {
		return nil, fmt.Errorf("forwarder: predicate must not be nil")
	}
	f := &Forwarder{
		predicate: predicate,
		target:    func(*Message) error { return nil },
	}
	for _, o := range opts {
		o(f)
	}
	return f, nil
}

// Apply evaluates the predicate against msg. When it matches, target is called
// and its error returned. Non-matching messages are returned as-is with no error.
func (f *Forwarder) Apply(msg *Message) (*Message, error) {
	if msg == nil {
		return nil, nil
	}
	if f.predicate(msg) {
		if err := f.target(msg); err != nil {
			return nil, fmt.Errorf("forwarder: target error: %w", err)
		}
	}
	return msg, nil
}
