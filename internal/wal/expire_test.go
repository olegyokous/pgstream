package wal

import (
	"testing"
	"time"
)

var expireBase = time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

func expireMsg(age time.Duration) *Message {
	return &Message{
		Table:        "orders",
		Action:       "INSERT",
		WalTimestamp: expireBase.Add(-age),
	}
}

func TestNewExpirer_ZeroTTLErrors(t *testing.T) {
	_, err := NewExpirer(ExpirerConfig{TTL: 0})
	if err == nil {
		t.Fatal("expected error for zero TTL")
	}
}

func TestNewExpirer_NegativeTTLErrors(t *testing.T) {
	_, err := NewExpirer(ExpirerConfig{TTL: -time.Second})
	if err == nil {
		t.Fatal("expected error for negative TTL")
	}
}

func TestExpirer_FreshMessagePassesThrough(t *testing.T) {
	e, _ := NewExpirer(ExpirerConfig{TTL: time.Minute},
		withExpirerClock(func() time.Time { return expireBase }))
	msg := expireMsg(10 * time.Second)
	if got := e.Apply(msg); got == nil {
		t.Fatal("expected message to pass through")
	}
}

func TestExpirer_ExpiredMessageDropped(t *testing.T) {
	e, _ := NewExpirer(ExpirerConfig{TTL: time.Minute},
		withExpirerClock(func() time.Time { return expireBase }))
	msg := expireMsg(2 * time.Minute)
	if got := e.Apply(msg); got != nil {
		t.Fatalf("expected nil for expired message, got %+v", got)
	}
}

func TestExpirer_NilMessagePassthrough(t *testing.T) {
	e, _ := NewExpirer(DefaultExpirerConfig())
	if got := e.Apply(nil); got != nil {
		t.Fatal("expected nil in → nil out")
	}
}

func TestExpirer_ZeroTimestampSkipsCheck(t *testing.T) {
	e, _ := NewExpirer(ExpirerConfig{TTL: time.Second},
		withExpirerClock(func() time.Time { return expireBase }))
	msg := &Message{Table: "orders", Action: "INSERT"} // zero WalTimestamp
	if got := e.Apply(msg); got == nil {
		t.Fatal("zero timestamp should not be dropped")
	}
}

func TestExpirer_TableScopedSkipsOtherTable(t *testing.T) {
	e, _ := NewExpirer(ExpirerConfig{TTL: time.Minute, Table: "orders"},
		withExpirerClock(func() time.Time { return expireBase }))
	msg := &Message{Table: "users", Action: "INSERT", WalTimestamp: expireBase.Add(-2 * time.Minute)}
	if got := e.Apply(msg); got == nil {
		t.Fatal("non-scoped table should not be dropped")
	}
}

func TestExpirer_DefaultConfig(t *testing.T) {
	cfg := DefaultExpirerConfig()
	if cfg.TTL <= 0 {
		t.Fatal("default TTL must be positive")
	}
}
