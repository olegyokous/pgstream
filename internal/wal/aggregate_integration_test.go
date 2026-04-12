package wal

import (
	"sync"
	"testing"
)

func TestAggregator_ConcurrentRecords(t *testing.T) {
	a, err := NewAggregator(DefaultAggregateConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	const goroutines = 20
	const msgsEach = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < msgsEach; j++ {
				a.Record(aggMsg("events", "INSERT"))
			}
		}()
	}
	wg.Wait()

	snap := a.Snapshot()
	want := goroutines * msgsEach
	if snap["events"] != want {
		t.Errorf("expected events=%d, got %d", want, snap["events"])
	}
}

func TestAggregator_MultipleTablesAndActions(t *testing.T) {
	byTable, _ := NewAggregator(AggregateConfig{GroupBy: "table"})
	byAction, _ := NewAggregator(AggregateConfig{GroupBy: "action"})

	msgs := []*Message{
		aggMsg("users", "INSERT"),
		aggMsg("users", "UPDATE"),
		aggMsg("products", "INSERT"),
		aggMsg("products", "DELETE"),
		aggMsg("orders", "INSERT"),
	}
	for _, m := range msgs {
		byTable.Record(m)
		byAction.Record(m)
	}

	tSnap := byTable.Snapshot()
	if tSnap["users"] != 2 || tSnap["products"] != 2 || tSnap["orders"] != 1 {
		t.Errorf("unexpected table counts: %v", tSnap)
	}

	aSnap := byAction.Snapshot()
	if aSnap["INSERT"] != 3 || aSnap["UPDATE"] != 1 || aSnap["DELETE"] != 1 {
		t.Errorf("unexpected action counts: %v", aSnap)
	}
}
