package wal

import (
	"errors"
	"sort"
)

// DefaultReordererConfig returns a sensible default configuration.
func DefaultReordererConfig() ReordererConfig {
	return ReordererConfig{
		BufferSize: 64,
		Field:      "lsn",
	}
}

// ReordererConfig controls how the Reorderer buffers and sorts messages.
type ReordererConfig struct {
	// BufferSize is the maximum number of messages held before a flush is forced.
	BufferSize int
	// Field is the meta key used to compare message ordering (e.g. "lsn" or "seq").
	Field string
}

// Reorderer accumulates messages and emits them in ascending order of a
// numeric meta field. When the buffer is full it flushes all held messages.
type Reorderer struct {
	cfg    ReordererConfig
	buffer []*Message
}

// NewReorderer creates a Reorderer with the supplied config.
func NewReorderer(cfg ReordererConfig) (*Reorderer, error) {
	if cfg.BufferSize <= 0 {
		return nil, errors.New("reorderer: BufferSize must be greater than zero")
	}
	if cfg.Field == "" {
		return nil, errors.New("reorderer: Field must not be empty")
	}
	return &Reorderer{
		cfg:    cfg,
		buffer: make([]*Message, 0, cfg.BufferSize),
	}, nil
}

// Add appends a message to the internal buffer. If the buffer reaches its
// capacity the method flushes and returns all messages sorted by Field.
// Otherwise it returns nil, signalling that more messages are expected.
func (r *Reorderer) Add(msg *Message) []*Message {
	if msg == nil {
		return nil
	}
	r.buffer = append(r.buffer, msg)
	if len(r.buffer) >= r.cfg.BufferSize {
		return r.Flush()
	}
	return nil
}

// Flush sorts and returns all buffered messages, clearing the internal buffer.
func (r *Reorderer) Flush() []*Message {
	if len(r.buffer) == 0 {
		return nil
	}
	field := r.cfg.Field
	sort.SliceStable(r.buffer, func(i, j int) bool {
		vi, _ := r.buffer[i].Meta[field].(uint64)
		vj, _ := r.buffer[j].Meta[field].(uint64)
		return vi < vj
	})
	out := make([]*Message, len(r.buffer))
	copy(out, r.buffer)
	r.buffer = r.buffer[:0]
	return out
}

// Len returns the number of messages currently buffered.
func (r *Reorderer) Len() int { return len(r.buffer) }
