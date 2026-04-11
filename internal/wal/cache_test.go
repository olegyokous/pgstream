package wal

import (
	"fmt"
	"testing"
	"time"
)

func fixedCacheNow(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestCache_SetAndGet(t *testing.T) {
	c := NewCache(DefaultCacheConfig())
	c.Set("k", "v")
	val, ok := c.Get("k")
	if !ok || val != "v" {
		t.Fatalf("expected (v, true), got (%q, %v)", val, ok)
	}
}

func TestCache_MissingKeyReturnsFalse(t *testing.T) {
	c := NewCache(DefaultCacheConfig())
	_, ok := c.Get("missing")
	if ok {
		t.Fatal("expected false for missing key")
	}
}

func TestCache_ExpiredKeyReturnsFalse(t *testing.T) {
	now := time.Now()
	c := NewCache(CacheConfig{TTL: time.Second, MaxSize: 10})
	c.clock = fixedCacheNow(now)
	c.Set("k", "v")
	// advance clock past TTL
	c.clock = fixedCacheNow(now.Add(2 * time.Second))
	_, ok := c.Get("k")
	if ok {
		t.Fatal("expected expired key to return false")
	}
}

func TestCache_Delete(t *testing.T) {
	c := NewCache(DefaultCacheConfig())
	c.Set("k", "v")
	c.Delete("k")
	_, ok := c.Get("k")
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestCache_Len(t *testing.T) {
	c := NewCache(DefaultCacheConfig())
	for i := 0; i < 5; i++ {
		c.Set(fmt.Sprintf("k%d", i), "v")
	}
	if c.Len() != 5 {
		t.Fatalf("expected 5, got %d", c.Len())
	}
}

func TestCache_EvictsOldestWhenFull(t *testing.T) {
	now := time.Now()
	c := NewCache(CacheConfig{TTL: time.Minute, MaxSize: 3})
	c.clock = fixedCacheNow(now)
	c.Set("a", "1")
	c.clock = fixedCacheNow(now.Add(1 * time.Second))
	c.Set("b", "2")
	c.clock = fixedCacheNow(now.Add(2 * time.Second))
	c.Set("c", "3")
	// Adding a 4th entry should evict "a" (oldest expiry)
	c.clock = fixedCacheNow(now.Add(3 * time.Second))
	c.Set("d", "4")
	if c.Len() != 3 {
		t.Fatalf("expected 3 entries after eviction, got %d", c.Len())
	}
	_, ok := c.Get("a")
	if ok {
		t.Fatal("expected 'a' to have been evicted")
	}
}

func TestCache_DefaultConfigFallback(t *testing.T) {
	c := NewCache(CacheConfig{TTL: 0, MaxSize: 0})
	if c.config.TTL != DefaultCacheConfig().TTL {
		t.Fatalf("expected default TTL, got %v", c.config.TTL)
	}
	if c.config.MaxSize != DefaultCacheConfig().MaxSize {
		t.Fatalf("expected default MaxSize, got %d", c.config.MaxSize)
	}
}
