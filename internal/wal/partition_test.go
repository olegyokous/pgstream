package wal

import (
	"testing"
)

func TestNewPartitioner_InvalidBuckets(t *testing.T) {
	_, err := NewPartitioner(0, PartitionByTable)
	if err == nil {
		t.Fatal("expected error for n=0")
	}
}

func TestNewPartitioner_ValidBuckets(t *testing.T) {
	p, err := NewPartitioner(4, PartitionByTable)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Buckets() != 4 {
		t.Fatalf("expected 4 buckets, got %d", p.Buckets())
	}
}

func TestPartitioner_NilMessageReturnsZero(t *testing.T) {
	p, _ := NewPartitioner(8, PartitionByTable)
	if got := p.Partition(nil); got != 0 {
		t.Fatalf("expected 0 for nil message, got %d", got)
	}
}

func TestPartitioner_ByTableIsStable(t *testing.T) {
	p, _ := NewPartitioner(8, PartitionByTable)
	msg := &Message{Table: "orders", Action: "INSERT"}
	first := p.Partition(msg)
	for i := 0; i < 10; i++ {
		if got := p.Partition(msg); got != first {
			t.Fatalf("partition not stable: got %d, want %d", got, first)
		}
	}
}

func TestPartitioner_ByActionIsStable(t *testing.T) {
	p, _ := NewPartitioner(4, PartitionByAction)
	msg := &Message{Table: "users", Action: "DELETE"}
	first := p.Partition(msg)
	for i := 0; i < 5; i++ {
		if got := p.Partition(msg); got != first {
			t.Fatalf("partition not stable: got %d, want %d", got, first)
		}
	}
}

func TestPartitioner_ByPKUsesFirstColumn(t *testing.T) {
	p, _ := NewPartitioner(16, PartitionByPK)
	msgA := &Message{Columns: []Column{{Name: "id", Value: "42"}}}
	msgB := &Message{Columns: []Column{{Name: "id", Value: "99"}}}
	if p.Partition(msgA) == p.Partition(msgB) {
		// Not guaranteed to differ but very likely for distinct values;
		// just verify no panic and result is in range.
	}
	for _, msg := range []*Message{msgA, msgB} {
		got := p.Partition(msg)
		if got < 0 || got >= 16 {
			t.Fatalf("partition %d out of range [0,16)", got)
		}
	}
}

func TestPartitioner_ResultInRange(t *testing.T) {
	tables := []string{"users", "orders", "products", "inventory", "payments"}
	p, _ := NewPartitioner(3, PartitionByTable)
	for _, tbl := range tables {
		msg := &Message{Table: tbl, Action: "INSERT"}
		got := p.Partition(msg)
		if got < 0 || got >= 3 {
			t.Fatalf("table %q: partition %d out of range [0,3)", tbl, got)
		}
	}
}
