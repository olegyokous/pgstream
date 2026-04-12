package wal

import (
	"errors"
	"sync"
)

// Sequencer assigns monotonically increasing sequence numbers to WAL messages,
// enabling downstream consumers to detect gaps or reordering.
type Sequencer struct {
	mu      sync.Mutex
	current uint64
	table   string // empty means apply to all tables
}

// SequencerOption configures a Sequencer.
type SequencerOption func(*Sequencer)

// WithSequencerTable restricts sequencing to messages from the given table.
func WithSequencerTable(table string) SequencerOption {
	return func(s *Sequencer) {
		s.table = table
	}
}

// WithSequencerStart sets the initial sequence value (default 0).
func WithSequencerStart(start uint64) SequencerOption {
	return func(s *Sequencer) {
		s.current = start
	}
}

// NewSequencer creates a Sequencer with the given options.
func NewSequencer(opts ...SequencerOption) (*Sequencer, error) {
	s := &Sequencer{}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// Stamp assigns the next sequence number to msg.Meta["seq"] and returns the
// updated message. If the sequencer is scoped to a table and msg.Table does
// not match, the message is returned unchanged.
func (s *Sequencer) Stamp(msg *Message) (*Message, error) {
	if msg == nil {
		return nil, errors.New("sequencer: nil message")
	}
	if s.table != "" && msg.Table != s.table {
		return msg, nil
	}

	s.mu.Lock()
	s.current++
	seq := s.current
	s.mu.Unlock()

	if msg.Meta == nil {
		msg.Meta = make(map[string]string)
	}
	msg.Meta["seq"] = uint64ToString(seq)
	return msg, nil
}

// Current returns the last sequence number assigned (0 if none yet).
func (s *Sequencer) Current() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}

// Reset resets the sequence counter to zero.
func (s *Sequencer) Reset() {
	s.mu.Lock()
	s.current = 0
	s.mu.Unlock()
}

// uint64ToString converts a uint64 to its decimal string representation
// without importing strconv at the call site.
func uint64ToString(n uint64) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
