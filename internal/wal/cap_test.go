package wal

import (
	"testing"
)

func capMsg(table, action string) *Message {
	return &Message{Table: table, Action: Action(action)}
}

func TestNewCapper_ZeroMaxErrors(t *testing.T) {
	_, err := NewCapper(0, "", "")
	if err == nil {
		t.Fatal("expected error for zero max")
	}
}

func TestNewCapper_NegativeMaxErrors(t *testing.T) {
	_, err := NewCapper(-5, "", "")
	if err == nil {
		t.Fatal("expected error for negative max")
	}
}

func TestCapper_AllowsUpToMax(t *testing.T) {
	c, _ := NewCapper(3, "", "")
	for i := 0; i < 3; i++ {
		if !c.Allow(capMsg("orders", "INSERT")) {
			t.Fatalf("expected message %d to be allowed", i+1)
		}
	}
	if c.Allow(capMsg("orders", "INSERT")) {
		t.Fatal("expected message 4 to be dropped")
	}
}

func TestCapper_NilMessageDropped(t *testing.T) {
	c, _ := NewCapper(10, "", "")
	if c.Allow(nil) {
		t.Fatal("expected nil message to be dropped")
	}
}

func TestCapper_ResetRestoresCount(t *testing.T) {
	c, _ := NewCapper(2, "", "")
	c.Allow(capMsg("t", "INSERT"))
	c.Allow(capMsg("t", "INSERT"))
	if c.Allow(capMsg("t", "INSERT")) {
		t.Fatal("should be capped before reset")
	}
	c.Reset()
	if !c.Allow(capMsg("t", "INSERT")) {
		t.Fatal("should be allowed after reset")
	}
}

func TestCapper_TableScopePassesOtherTables(t *testing.T) {
	c, _ := NewCapper(1, "orders", "")
	c.Allow(capMsg("orders", "INSERT")) // consumes the cap
	// different table should pass through regardless
	if !c.Allow(capMsg("users", "INSERT")) {
		t.Fatal("out-of-scope table should pass through")
	}
}

func TestCapper_ActionScopePassesOtherActions(t *testing.T) {
	c, _ := NewCapper(1, "", "DELETE")
	c.Allow(capMsg("t", "DELETE")) // consumes the cap
	if !c.Allow(capMsg("t", "INSERT")) {
		t.Fatal("out-of-scope action should pass through")
	}
}

func TestCapper_RemainingDecrementsCorrectly(t *testing.T) {
	c, _ := NewCapper(3, "", "")
	if c.Remaining() != 3 {
		t.Fatalf("expected 3, got %d", c.Remaining())
	}
	c.Allow(capMsg("t", "INSERT"))
	if c.Remaining() != 2 {
		t.Fatalf("expected 2, got %d", c.Remaining())
	}
}
