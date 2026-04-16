package wal

import (
	"errors"
	"sync"
)

// Pinner holds a set of message keys that are "pinned" — they bypass
// downstream filtering and are always forwarded regardless of other rules.
type Pinner struct {
	mu   sync.RWMutex
	keys map[string]struct{}
	keyFn func(*Message) string
}

type PinnerOption func(*Pinner)

func WithPinnerKeyFn(fn func(*Message) string) PinnerOption {
	return func(p *Pinner) { p.keyFn = fn }
}

// NewPinner creates a Pinner. keyFn extracts the pin key from a message.
func NewPinner(opts ...PinnerOption) (*Pinner, error) {
	p := &Pinner{
		keys:  make(map[string]struct{}),
		keyFn: func(m *Message) string { return m.Table },
	}
	for _, o := range opts {
		o(p)
	}
	if p.keyFn == nil {
		return nil, errors.New("pinner: keyFn must not be nil")
	}
	return p, nil
}

// Pin marks a key as pinned.
func (p *Pinner) Pin(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys[key] = struct{}{}
}

// Unpin removes a key from the pinned set.
func (p *Pinner) Unpin(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.keys, key)
}

// IsPinned reports whether the message's key is currently pinned.
func (p *Pinner) IsPinned(m *Message) bool {
	if m == nil {
		return false
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.keys[p.keyFn(m)]
	return ok
}

// Len returns the number of pinned keys.
func (p *Pinner) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.keys)
}
