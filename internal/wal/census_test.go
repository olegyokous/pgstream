package wal

import (
	"strings"
	"testing"
)

func censusMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestCensus_InitialisesEmpty(t *testing.T) {
	c := NewCensus()
	if got := c.Count("users", "INSERT"); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
	if len(c.Tables()) != 0 {
		t.Fatal("expected no tables")
	}
}

func TestCensus_RecordIncrements(t *testing.T) {
	c := NewCensus()
	c.Record(censusMsg("users", "INSERT"))
	c.Record(censusMsg("users", "INSERT"))
	c.Record(censusMsg("users", "UPDATE"))
	if got := c.Count("users", "INSERT"); got != 2 {
		t.Fatalf("expected 2, got %d", got)
	}
	if got := c.Count("users", "UPDATE"); got != 1 {
		t.Fatalf("expected 1, got %d", got)
	}
}

func TestCensus_NilMessageIsNoop(t *testing.T) {
	c := NewCensus()
	c.Record(nil) // must not panic
	if len(c.Tables()) != 0 {
		t.Fatal("expected no tables after nil record")
	}
}

func TestCensus_TablesReturnsAllSeen(t *testing.T) {
	c := NewCensus()
	c.Record(censusMsg("orders", "INSERT"))
	c.Record(censusMsg("products", "DELETE"))
	tables := c.Tables()
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}
}

func TestCensus_Reset(t *testing.T) {
	c := NewCensus()
	c.Record(censusMsg("users", "INSERT"))
	c.Reset()
	if got := c.Count("users", "INSERT"); got != 0 {
		t.Fatalf("expected 0 after reset, got %d", got)
	}
	if len(c.Tables()) != 0 {
		t.Fatal("expected no tables after reset")
	}
}

func TestCensus_SummaryEmpty(t *testing.T) {
	c := NewCensus()
	if !strings.Contains(c.Summary(), "no records") {
		t.Fatal("expected 'no records' in empty summary")
	}
}

func TestCensus_SummaryContainsEntries(t *testing.T) {
	c := NewCensus()
	c.Record(censusMsg("events", "INSERT"))
	s := c.Summary()
	if !strings.Contains(s, "events") {
		t.Fatalf("expected 'events' in summary, got: %s", s)
	}
	if !strings.Contains(s, "INSERT") {
		t.Fatalf("expected 'INSERT' in summary, got: %s", s)
	}
}
