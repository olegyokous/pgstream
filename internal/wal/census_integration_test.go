package wal

import (
	"sync"
	"testing"
)

func TestCensus_ConcurrentRecordsAreSafe(t *testing.T) {
	c := NewCensus()
	var wg sync.WaitGroup
	const goroutines = 50
	const recordsEach = 100

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < recordsEach; j++ {
				c.Record(censusMsg("users", "INSERT"))
			}
		}()
	}
	wg.Wait()

	expected := int64(goroutines * recordsEach)
	if got := c.Count("users", "INSERT"); got != expected {
		t.Fatalf("expected %d, got %d", expected, got)
	}
}

func TestCensus_MultipleTablesAndActions(t *testing.T) {
	c := NewCensus()
	tables := []string{"users", "orders", "products"}
	actions := []string{"INSERT", "UPDATE", "DELETE"}

	for _, tbl := range tables {
		for _, act := range actions {
			for i := 0; i < 5; i++ {
				c.Record(censusMsg(tbl, act))
			}
		}
	}

	for _, tbl := range tables {
		for _, act := range actions {
			if got := c.Count(tbl, act); got != 5 {
				t.Errorf("table=%s action=%s: expected 5, got %d", tbl, act, got)
			}
		}
	}

	if len(c.Tables()) != len(tables) {
		t.Fatalf("expected %d tables, got %d", len(tables), len(c.Tables()))
	}
}
