package wal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// ContentHasherConfig holds configuration for the content-based deduplicator.
type ContentHasherConfig struct {
	TTL      time.Duration
	MaxSize  int
	Columns  []string // columns to include in hash; empty means all
}

func DefaultContentHasherConfig() ContentHasherConfig {
	return ContentHasherConfig{
		TTL:     5 * time.Minute,
		MaxSize: 10_000,
	}
}

type hashEntry struct {
	expiry time.Time
}

// ContentHasher drops messages whose column fingerprint was seen recently.
type ContentHasher struct {
	cfg   ContentHasherConfig
	mu    sync.Mutex
	seen  map[string]hashEntry
	clock func() time.Time
}

// NewContentHasher returns a ContentHasher with the given config.
func NewContentHasher(cfg ContentHasherConfig) (*ContentHasher, error) {
	if cfg.TTL <= 0 {
		return nil, fmt.Errorf("content hasher: TTL must be positive")
	}
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = DefaultContentHasherConfig().MaxSize
	}
	return &ContentHasher{
		cfg:   cfg,
		seen:  make(map[string]hashEntry),
		clock: time.Now,
	}, nil
}

func withContentHasherClock(c func() time.Time) func(*ContentHasher) {
	return func(h *ContentHasher) { h.clock = c }
}

// IsDuplicate returns true if the message content was seen within TTL.
func (h *ContentHasher) IsDuplicate(msg *Message) bool {
	if msg == nil {
		return false
	}
	key := h.fingerprint(msg)
	now := h.clock()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.evict(now)
	if e, ok := h.seen[key]; ok && now.Before(e.expiry) {
		return true
	}
	if len(h.seen) >= h.cfg.MaxSize {
		h.evictOldest()
	}
	h.seen[key] = hashEntry{expiry: now.Add(h.cfg.TTL)}
	return false
}

func (h *ContentHasher) fingerprint(msg *Message) string {
	sha := sha256.New()
	fmt.Fprintf(sha, "%s:%s:", msg.Table, msg.Action)
	for _, col := range msg.Columns {
		if len(h.cfg.Columns) > 0 && !h.columnIncluded(col.Name) {
			continue
		}
		fmt.Fprintf(sha, "%s=%v;", col.Name, col.Value)
	}
	return hex.EncodeToString(sha.Sum(nil))
}

func (h *ContentHasher) columnIncluded(name string) bool {
	for _, c := range h.cfg.Columns {
		if c == name {
			return true
		}
	}
	return false
}

func (h *ContentHasher) evict(now time.Time) {
	for k, e := range h.seen {
		if now.After(e.expiry) {
			delete(h.seen, k)
		}
	}
}

func (h *ContentHasher) evictOldest() {
	var oldest string
	var oldestTime time.Time
	for k, e := range h.seen {
		if oldest == "" || e.expiry.Before(oldestTime) {
			oldest = k
			oldestTime = e.expiry
		}
	}
	if oldest != "" {
		delete(h.seen, oldest)
	}
}
