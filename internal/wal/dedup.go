package wal

import (
	"sync"
	"time"
)

// DedupConfig holds configuration for the deduplicator.
type DedupConfig struct {
	// TTL is how long a seen message key is retained.
	TTL time.Duration
	// MaxSize is the maximum number of keys to track (0 = unlimited).
	MaxSize int
}

// DefaultDedupConfig returns a sensible default dedup configuration.
func DefaultDedupConfig() DedupConfig {
	return DedupConfig{
		TTL:     30 * time.Second,
		MaxSize: 10_000,
	}
}

type entry struct {
	expiresAt time.Time
}

// Deduplicator suppresses duplicate WAL messages within a rolling TTL window.
type Deduplicator struct {
	mu      sync.Mutex
	seen    map[string]entry
	cfg     DedupConfig
	nowFunc func() time.Time
}

// NewDeduplicator creates a Deduplicator with the given config.
func NewDeduplicator(cfg DedupConfig) *Deduplicator {
	return &Deduplicator{
		seen:    make(map[string]entry),
		cfg:     cfg,
		nowFunc: time.Now,
	}
}

// IsDuplicate returns true if the key was seen within the TTL window.
// If the key is new (or expired), it is recorded and false is returned.
func (d *Deduplicator) IsDuplicate(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := d.nowFunc()
	if e, ok := d.seen[key]; ok && now.Before(e.expiresAt) {
		return true
	}

	// Evict expired entries when we are at capacity.
	if d.cfg.MaxSize > 0 && len(d.seen) >= d.cfg.MaxSize {
		d.evictExpired(now)
	}

	d.seen[key] = entry{expiresAt: now.Add(d.cfg.TTL)}
	return false
}

// Size returns the current number of tracked keys (including expired ones not
// yet evicted).
func (d *Deduplicator) Size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}

// evictExpired removes all expired entries. Caller must hold d.mu.
func (d *Deduplicator) evictExpired(now time.Time) {
	for k, e := range d.seen {
		if now.After(e.expiresAt) {
			delete(d.seen, k)
		}
	}
}
