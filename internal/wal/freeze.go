package wal

import (
	"errors"
	"sync"
)

// Freezer temporarily halts message processing when frozen, buffering messages
// up to a configurable limit and releasing them when thawed.
type Freezer struct {
	mu      sync.Mutex
	frozen  bool
	buf     []*Message
	maxBuf  int
}

// NewFreezer returns a Freezer with the given buffer limit.
// maxBuf must be greater than zero.
func NewFreezer(maxBuf int) (*Freezer, error) {
	if maxBuf <= 0 {
		return nil, errors.New("freezer: maxBuf must be greater than zero")
	}
	return &Freezer{maxBuf: maxBuf}, nil
}

// Freeze halts message forwarding; subsequent Apply calls buffer messages.
func (f *Freezer) Freeze() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frozen = true
}

// Thaw releases the freeze and returns all buffered messages in order.
// The internal buffer is cleared.
func (f *Freezer) Thaw() []*Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frozen = false
	out := make([]*Message, len(f.buf))
	copy(out, f.buf)
	f.buf = f.buf[:0]
	return out
}

// Apply returns the message when not frozen. When frozen, the message is
// buffered (up to maxBuf; oldest are dropped when full) and nil is returned.
func (f *Freezer) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.frozen {
		return msg
	}
	if len(f.buf) >= f.maxBuf {
		// drop oldest
		f.buf = f.buf[1:]
	}
	f.buf = append(f.buf, msg)
	return nil
}

// IsFrozen reports whether the Freezer is currently frozen.
func (f *Freezer) IsFrozen() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.frozen
}

// Len returns the number of buffered messages.
func (f *Freezer) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.buf)
}
