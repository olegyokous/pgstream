package wal

import "errors"

// Splitter divides a stream of messages into two output channels based on a
// predicate. Messages matching the predicate are sent to the Left channel;
// all others are sent to the Right channel.
type Splitter struct {
	predicate func(*Message) bool
	Left      chan *Message
	Right     chan *Message
}

// SplitterConfig holds options for NewSplitter.
type SplitterConfig struct {
	// BufferSize controls the capacity of each output channel (default 64).
	BufferSize int
}

// DefaultSplitterConfig returns a SplitterConfig with sensible defaults.
func DefaultSplitterConfig() SplitterConfig {
	return SplitterConfig{BufferSize: 64}
}

// NewSplitter creates a Splitter that routes messages using predicate.
// Messages where predicate returns true go to Left; others go to Right.
func NewSplitter(predicate func(*Message) bool, cfg SplitterConfig) (*Splitter, error) {
	if predicate == nil {
		return nil, errors.New("splitter: predicate must not be nil")
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = DefaultSplitterConfig().BufferSize
	}
	return &Splitter{
		predicate: predicate,
		Left:      make(chan *Message, cfg.BufferSize),
		Right:     make(chan *Message, cfg.BufferSize),
	}, nil
}

// Dispatch sends msg to either Left or Right based on the predicate.
// Returns an error if msg is nil.
func (s *Splitter) Dispatch(msg *Message) error {
	if msg == nil {
		return errors.New("splitter: cannot dispatch nil message")
	}
	if s.predicate(msg) {
		s.Left <- msg
	} else {
		s.Right <- msg
	}
	return nil
}

// Close closes both output channels, signalling consumers that no more
// messages will be sent.
func (s *Splitter) Close() {
	close(s.Left)
	close(s.Right)
}
