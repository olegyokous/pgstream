package wal

import (
	"testing"
)

func TestCensus_RecordsFromMessage(t *testing.T) {
	c := NewCensus()

	msgs := []*Message{
		censusMsg("users", "INSERT"),
		censusMsg("users", "INSERT"),
		censusMsg("orders", "DELETE"),
	}

	for _, m := range msgs {
		c.Record(m)
	}

	if got := c.Count("users", "INSERT"); got != 2 {
		t.Fatalf("users/INSERT: expected 2, got %d", got)
	}
	if got := c.Count("orders", "DELETE"); got != 1 {
		t.Fatalf("orders/DELETE: expected 1, got %d", got)
	}
	if got := c.Count("orders", "INSERT"); got != 0 {
		t.Fatalf("orders/INSERT: expected 0, got %d", got)
	}
}

func TestCensus_ResetAndReRecord(t *testing.T) {
	c := NewCensus()

	c.Record(censusMsg("users", "UPDATE"))
	c.Record(censusMsg("users", "UPDATE"))

	if got := c.Count("users", "UPDATE"); got != 2 {
		t.Fatalf("before reset: expected 2, got %d", got)
	}

	c.Reset()

	if got := c.Count("users", "UPDATE"); got != 0 {
		t.Fatalf("after reset: expected 0, got %d", got)
	}

	c.Record(censusMsg("users", "UPDATE"))

	if got := c.Count("users", "UPDATE"); got != 1 {
		t.Fatalf("after re-record: expected 1, got %d", got)
	}
}

func TestCensus_UnknownTableAndActionReturnsZero(t *testing.T) {
	c := NewCensus()
	c.Record(censusMsg("users", "INSERT"))

	if got := c.Count("nonexistent", "INSERT"); got != 0 {
		t.Fatalf("expected 0 for unknown table, got %d", got)
	}
	if got := c.Count("users", "DELETE"); got != 0 {
		t.Fatalf("expected 0 for unknown action, got %d", got)
	}
}
