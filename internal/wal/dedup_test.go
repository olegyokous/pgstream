package wal

import (
	"fmt"
	"testing"
	"time"
)

func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestDedup_NewKeyIsNotDuplicate(t *testing.T) {
	d := NewDeduplicator(DefaultDedupConfig())
	if d.IsDuplicate("key1") {
		t.Fatal("expected false for unseen key")
	}
}

func TestDedup_SameKeyWithinTTLIsDuplicate(t *testing.T) {
	d := NewDeduplicator(DefaultDedupConfig())
	d.IsDuplicate("key1") // record it
	if !d.IsDuplicate("key1") {
		t.Fatal("expected true for key seen within TTL")
	}
}

func TestDedup_ExpiredKeyIsNotDuplicate(t *testing.T) {
	now := time.Now()
	cfg := DedupConfig{TTL: 5 * time.Second, MaxSize: 100}
	d := NewDeduplicator(cfg)
	d.nowFunc = fixedClock(now)
	d.IsDuplicate("key1")

	// Advance clock past TTL.
	d.nowFunc = fixedClock(now.Add(10 * time.Second))
	if d.IsDuplicate("key1") {
		t.Fatal("expected false for key past TTL")
	}
}

func TestDedup_DifferentKeysAreIndependent(t *testing.T) {
	d := NewDeduplicator(DefaultDedupConfig())
	d.IsDuplicate("a")
	if d.IsDuplicate("b") {
		t.Fatal("key 'b' should not be a duplicate of 'a'")
	}
}

func TestDedup_SizeTracksEntries(t *testing.T) {
	d := NewDeduplicator(DefaultDedupConfig())
	for i := 0; i < 5; i++ {
		d.IsDuplicate(fmt.Sprintf("key%d", i))
	}
	if d.Size() != 5 {
		t.Fatalf("expected size 5, got %d", d.Size())
	}
}

func TestDedup_MaxSizeEvictsExpired(t *testing.T) {
	now := time.Now()
	cfg := DedupConfig{TTL: 5 * time.Second, MaxSize: 3}
	d := NewDeduplicator(cfg)
	d.nowFunc = fixedClock(now)

	// Fill to capacity.
	d.IsDuplicate("a")
	d.IsDuplicate("b")
	d.IsDuplicate("c")

	// Advance past TTL so existing entries are expired.
	d.nowFunc = fixedClock(now.Add(10 * time.Second))

	// Adding a new key should evict expired ones and succeed.
	if d.IsDuplicate("d") {
		t.Fatal("expected false for new key after eviction")
	}
}
