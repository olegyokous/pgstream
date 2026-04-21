package wal

import (
	"errors"
	"sync"
)

// Stash temporarily holds messages keyed by an arbitrary string, allowing
// later retrieval or eviction. It is safe for concurrent use.
type Stash struct {
	mu      sync.RWMutex
	entries map[string]*Message
	maxSize int
}

// DefaultStashSize is the maximum number of entries held when no size is given.
const DefaultStashSize = 256

// NewStash creates a Stash with the given maximum capacity.
// If maxSize is zero or negative the DefaultStashSize is used.
func NewStash(maxSize int) (*Stash, error) {
	if maxSize <= 0 {
		maxSize = DefaultStashSize
	}
	return &Stash{
		entries: make(map[string]*Message, maxSize),
		maxSize: maxSize,
	}, nil
}

// Put stores msg under key. Returns an error when the stash is full and the
// key is not already present.
func (s *Stash) Put(key string, msg *Message) error {
	if key == "" {
		return errors.New("stash: key must not be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.entries[key]
	if !exists && len(s.entries) >= s.maxSize {
		return errors.New("stash: capacity exceeded")
	}
	s.entries[key] = msg
	return nil
}

// Get retrieves the message stored under key. The second return value reports
// whether the key was present.
func (s *Stash) Get(key string) (*Message, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.entries[key]
	return m, ok
}

// Pop retrieves and removes the message stored under key.
func (s *Stash) Pop(key string) (*Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.entries[key]
	if ok {
		delete(s.entries, key)
	}
	return m, ok
}

// Len returns the current number of entries in the stash.
func (s *Stash) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// Flush removes all entries and returns them as a map snapshot.
func (s *Stash) Flush() map[string]*Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*Message, len(s.entries))
	for k, v := range s.entries {
		out[k] = v
	}
	s.entries = make(map[string]*Message, s.maxSize)
	return out
}
