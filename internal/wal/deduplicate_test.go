package wal

import (
	"testing"
	"time"
)

func dupMsg(table, action string, cols ...Column) *Message {
	return &Message{Table: table, Action: action, Columns: cols}
}

func TestNewContentHasher_ZeroTTLErrors(t *testing.T) {
	_, err := NewContentHasher(ContentHasherConfig{TTL: 0, MaxSize: 10})
	if err == nil {
		t.Fatal("expected error for zero TTL")
	}
}

func TestNewContentHasher_ValidConfig(t *testing.T) {
	h, err := NewContentHasher(DefaultContentHasherConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h == nil {
		t.Fatal("expected non-nil hasher")
	}
}

func TestContentHasher_FirstMessageIsNotDuplicate(t *testing.T) {
	h, _ := NewContentHasher(DefaultContentHasherConfig())
	msg := dupMsg("users", "INSERT", Column{Name: "id", Value: 1})
	if h.IsDuplicate(msg) {
		t.Fatal("first message should not be duplicate")
	}
}

func TestContentHasher_SameMessageIsDuplicate(t *testing.T) {
	h, _ := NewContentHasher(DefaultContentHasherConfig())
	msg := dupMsg("users", "INSERT", Column{Name: "id", Value: 1})
	h.IsDuplicate(msg)
	if !h.IsDuplicate(dupMsg("users", "INSERT", Column{Name: "id", Value: 1})) {
		t.Fatal("identical message should be duplicate")
	}
}

func TestContentHasher_ExpiredMessageIsNotDuplicate(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	clock := func() time.Time { return now }
	h, _ := NewContentHasher(ContentHasherConfig{TTL: 1 * time.Second, MaxSize: 100})
	withContentHasherClock(clock)(h)
	msg := dupMsg("orders", "UPDATE", Column{Name: "status", Value: "paid"})
	h.IsDuplicate(msg)
	now = now.Add(2 * time.Second)
	if h.IsDuplicate(dupMsg("orders", "UPDATE", Column{Name: "status", Value: "paid"})) {
		t.Fatal("expired entry should not be duplicate")
	}
}

func TestContentHasher_DifferentValuesAreNotDuplicate(t *testing.T) {
	h, _ := NewContentHasher(DefaultContentHasherConfig())
	h.IsDuplicate(dupMsg("users", "INSERT", Column{Name: "id", Value: 1}))
	if h.IsDuplicate(dupMsg("users", "INSERT", Column{Name: "id", Value: 2})) {
		t.Fatal("different column value should not be duplicate")
	}
}

func TestContentHasher_NilMessageReturnsFalse(t *testing.T) {
	h, _ := NewContentHasher(DefaultContentHasherConfig())
	if h.IsDuplicate(nil) {
		t.Fatal("nil message should return false")
	}
}

func TestContentHasher_ColumnFilterLimitsFingerprint(t *testing.T) {
	h, _ := NewContentHasher(ContentHasherConfig{
		TTL:     time.Minute,
		MaxSize: 100,
		Columns: []string{"id"},
	})
	// same id, different name — should be duplicate because name is excluded
	h.IsDuplicate(dupMsg("users", "INSERT",
		Column{Name: "id", Value: 42},
		Column{Name: "name", Value: "alice"},
	))
	if !h.IsDuplicate(dupMsg("users", "INSERT",
		Column{Name: "id", Value: 42},
		Column{Name: "name", Value: "bob"},
	)) {
		t.Fatal("messages with same filtered columns should be duplicate")
	}
}

func TestContentHasher_MaxSizeEvictsOldest(t *testing.T) {
	h, _ := NewContentHasher(ContentHasherConfig{TTL: time.Hour, MaxSize: 2})
	h.IsDuplicate(dupMsg("t", "INSERT", Column{Name: "id", Value: 1}))
	h.IsDuplicate(dupMsg("t", "INSERT", Column{Name: "id", Value: 2}))
	// third insert forces eviction of oldest
	h.IsDuplicate(dupMsg("t", "INSERT", Column{Name: "id", Value: 3}))
	// map size should not exceed MaxSize
	h.mu.Lock()
	size := len(h.seen)
	h.mu.Unlock()
	if size > h.cfg.MaxSize {
		t.Fatalf("seen map size %d exceeds MaxSize %d", size, h.cfg.MaxSize)
	}
}
