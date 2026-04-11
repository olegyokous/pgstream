package wal

import (
	"sync"
	"time"
)

// CacheConfig holds configuration for the TTL cache.
type CacheConfig struct {
	TTL             time.Duration
	MaxSize         int
	CleanupInterval time.Duration
}

// DefaultCacheConfig returns sensible defaults for the TTL cache.
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		TTL:             5 * time.Minute,
		MaxSize:         1024,
		CleanupInterval: 30 * time.Second,
	}
}

type cacheEntry struct {
	value     string
	expiresAt time.Time
}

// Cache is a simple thread-safe TTL key/value store.
type Cache struct {
	mu      sync.RWMutex
	config  CacheConfig
	entries map[string]cacheEntry
	clock   func() time.Time
}

// NewCache creates a Cache with the provided config.
func NewCache(cfg CacheConfig) *Cache {
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultCacheConfig().TTL
	}
	if cfg.MaxSize <= 0 {
		cfg.MaxSize = DefaultCacheConfig().MaxSize
	}
	return &Cache{
		config:  cfg,
		entries: make(map[string]cacheEntry),
		clock:   time.Now,
	}
}

// Set stores a value under key, replacing any existing entry.
func (c *Cache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.config.MaxSize {
		c.evictOldest()
	}
	c.entries[key] = cacheEntry{
		value:     value,
		expiresAt: c.clock().Add(c.config.TTL),
	}
}

// Get retrieves a value by key. Returns the value and whether it was found and not expired.
func (c *Cache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[key]
	if !ok || c.clock().After(e.expiresAt) {
		return "", false
	}
	return e.value, true
}

// Delete removes a key from the cache.
func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// Len returns the number of entries currently in the cache (including expired).
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// evictOldest removes the entry with the earliest expiry. Must be called with write lock held.
func (c *Cache) evictOldest() {
	var oldest string
	var oldestTime time.Time
	for k, e := range c.entries {
		if oldest == "" || e.expiresAt.Before(oldestTime) {
			oldest = k
			oldestTime = e.expiresAt
		}
	}
	if oldest != "" {
		delete(c.entries, oldest)
	}
}
