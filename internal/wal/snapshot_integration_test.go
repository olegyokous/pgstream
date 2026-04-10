package wal

import (
	"sync"
	"testing"
)

func TestSnapshotCollector_ConcurrentRecords(t *testing.T) {
	sc := NewSnapshotCollector()
	const goroutines = 20
	const recordsEach = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < recordsEach; j++ {
				sc.Record("events", "INSERT")
			}
		}()
	}
	wg.Wait()

	expected := int64(goroutines * recordsEach)
	if sc.Total() != expected {
		t.Fatalf("expected %d total, got %d", expected, sc.Total())
	}
}

func TestSnapshotCollector_MultipleTablesAndActions(t *testing.T) {
	sc := NewSnapshotCollector()

	tables := []string{"users", "orders", "products"}
	actions := []string{"INSERT", "UPDATE", "DELETE"}

	for _, tbl := range tables {
		for _, act := range actions {
			sc.Record(tbl, act)
			sc.Record(tbl, act)
		}
	}

	snap := sc.Snapshot()
	expectedKeys := len(tables) * len(actions)
	if len(snap.Counts) != expectedKeys {
		t.Fatalf("expected %d keys, got %d", expectedKeys, len(snap.Counts))
	}
	for _, tbl := range tables {
		for _, act := range actions {
			key := tbl + ":" + act
			if snap.Counts[key] != 2 {
				t.Errorf("key %s: expected 2, got %d", key, snap.Counts[key])
			}
		}
	}
}
