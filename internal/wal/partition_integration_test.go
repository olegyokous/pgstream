package wal

import (
	"sync"
	"testing"
)

func TestPartitioner_DistributesAcrossBuckets(t *testing.T) {
	p, err := NewPartitioner(4, PartitionByTable)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tables := []string{"alpha", "beta", "gamma", "delta", "epsilon",
		"zeta", "eta", "theta", "iota", "kappa"}

	counts := make([]int, 4)
	for _, tbl := range tables {
		msg := &Message{Table: tbl, Action: "INSERT"}
		counts[p.Partition(msg)]++
	}

	// At least 2 distinct buckets should be used for 10 varied table names.
	used := 0
	for _, c := range counts {
		if c > 0 {
			used++
		}
	}
	if used < 2 {
		t.Fatalf("expected at least 2 buckets used, got %d", used)
	}
}

func TestPartitioner_ConcurrentPartitionIsSafe(t *testing.T) {
	p, _ := NewPartitioner(8, PartitionByTable)
	msgs := []*Message{
		{Table: "users", Action: "INSERT"},
		{Table: "orders", Action: "UPDATE"},
		{Table: "products", Action: "DELETE"},
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		for _, msg := range msgs {
			wg.Add(1)
			go func(m *Message) {
				defer wg.Done()
				got := p.Partition(m)
				if got < 0 || got >= 8 {
					t.Errorf("partition %d out of range [0,8)", got)
				}
			}(msg)
		}
	}
	wg.Wait()
}
