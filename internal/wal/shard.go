package wal

import (
	"errors"
	"fmt"
	"sync"
)

// Sharder distributes messages across a fixed number of named shards based on a
// key function. Each shard maintains its own ordered slice of messages, allowing
// consumers to process partitions independently.
type Sharder struct {
	mu      sync.RWMutex
	shards  map[string][]*Message
	keyFn   func(*Message) string
	names   []string
}

// ShardOption configures a Sharder.
type ShardOption func(*Sharder)

// WithShardKeyFn sets a custom key function used to assign messages to shards.
// If the returned key does not match any shard name the message is dropped.
func WithShardKeyFn(fn func(*Message) string) ShardOption {
	return func(s *Sharder) {
		s.keyFn = fn
	}
}

// NewSharder creates a Sharder with the given shard names. At least one name
// must be provided. By default messages are sharded by table name.
func NewSharder(names []string, opts ...ShardOption) (*Sharder, error) {
	if len(names) == 0 {
		return nil, errors.New("sharder: at least one shard name is required")
	}

	shards := make(map[string][]*Message, len(names))
	for _, n := range names {
		if n == "" {
			return nil, errors.New("sharder: shard name must not be empty")
		}
		shards[n] = nil
	}

	s := &Sharder{
		shards: shards,
		names:  names,
		keyFn:  defaultShardKey,
	}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

// defaultShardKey returns the table name of the message.
func defaultShardKey(m *Message) string {
	if m == nil {
		return ""
	}
	return m.Table
}

// Assign places the message into the shard identified by the key function.
// If the key does not correspond to a known shard the message is silently
// dropped and false is returned.
func (s *Sharder) Assign(m *Message) bool {
	if m == nil {
		return false
	}
	key := s.keyFn(m)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.shards[key]; !ok {
		return false
	}
	s.shards[key] = append(s.shards[key], m)
	return true
}

// Drain removes and returns all messages currently held in the named shard.
// An error is returned if the shard name is unknown.
func (s *Sharder) Drain(name string) ([]*Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs, ok := s.shards[name]
	if !ok {
		return nil, fmt.Errorf("sharder: unknown shard %q", name)
	}
	s.shards[name] = nil
	return msgs, nil
}

// Len returns the number of messages currently queued in the named shard.
// It returns -1 if the shard name is unknown.
func (s *Sharder) Len(name string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msgs, ok := s.shards[name]
	if !ok {
		return -1
	}
	return len(msgs)
}

// Names returns the ordered list of shard names this Sharder was created with.
func (s *Sharder) Names() []string {
	result := make([]string, len(s.names))
	copy(result, s.names)
	return result
}
