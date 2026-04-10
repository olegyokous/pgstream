package wal

import (
	"errors"
	"sync"
)

// ErrBufferFull is returned when the ring buffer has no available capacity.
var ErrBufferFull = errors.New("ring buffer full")

// ErrBufferEmpty is returned when the ring buffer has no items to read.
var ErrBufferEmpty = errors.New("ring buffer empty")

// RingBuffer is a fixed-capacity, thread-safe circular buffer for WAL messages.
type RingBuffer struct {
	mu       sync.Mutex
	buf      []*Message
	head     int
	tail     int
	size     int
	capacity int
}

// NewRingBuffer allocates a new RingBuffer with the given capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 64
	}
	return &RingBuffer{
		buf:      make([]*Message, capacity),
		capacity: capacity,
	}
}

// Push adds a message to the buffer. Returns ErrBufferFull if at capacity.
func (r *RingBuffer) Push(msg *Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size == r.capacity {
		return ErrBufferFull
	}
	r.buf[r.tail] = msg
	r.tail = (r.tail + 1) % r.capacity
	r.size++
	return nil
}

// Pop removes and returns the oldest message. Returns ErrBufferEmpty if empty.
func (r *RingBuffer) Pop() (*Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size == 0 {
		return nil, ErrBufferEmpty
	}
	msg := r.buf[r.head]
	r.buf[r.head] = nil
	r.head = (r.head + 1) % r.capacity
	r.size--
	return msg, nil
}

// Len returns the current number of items in the buffer.
func (r *RingBuffer) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.size
}

// Cap returns the maximum capacity of the buffer.
func (r *RingBuffer) Cap() int {
	return r.capacity
}
