package wal

import (
	"errors"
	"sync"
)

// DefaultBufferConfig returns a BufferConfig with sensible defaults.
func DefaultBufferConfig() BufferConfig {
	return BufferConfig{
		Capacity: 256,
	}
}

// BufferConfig holds configuration for the MessageBuffer.
type BufferConfig struct {
	Capacity int
}

// MessageBuffer is a thread-safe, bounded FIFO buffer for WAL messages.
type MessageBuffer struct {
	mu       sync.Mutex
	items    []*Message
	cap      int
}

// NewMessageBuffer creates a new MessageBuffer with the given config.
func NewMessageBuffer(cfg BufferConfig) (*MessageBuffer, error) {
	if cfg.Capacity <= 0 {
		return nil, errors.New("buffer: capacity must be greater than zero")
	}
	return &MessageBuffer{
		items: make([]*Message, 0, cfg.Capacity),
		cap:  cfg.Capacity,
	}, nil
}

// Push appends a message to the buffer. Returns an error if the buffer is full.
func (b *MessageBuffer) Push(msg *Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.items) >= b.cap {
		return errors.New("buffer: capacity exceeded")
	}
	b.items = append(b.items, msg)
	return nil
}

// Pop removes and returns the oldest message. Returns nil if empty.
func (b *MessageBuffer) Pop() *Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.items) == 0 {
		return nil
	}
	msg := b.items[0]
	b.items = b.items[1:]
	return msg
}

// Len returns the current number of messages in the buffer.
func (b *MessageBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items)
}

// Drain removes and returns all messages currently in the buffer.
func (b *MessageBuffer) Drain() []*Message {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*Message, len(b.items))
	copy(out, b.items)
	b.items = b.items[:0]
	return out
}

// IsFull reports whether the buffer has reached capacity.
func (b *MessageBuffer) IsFull() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.items) >= b.cap
}
