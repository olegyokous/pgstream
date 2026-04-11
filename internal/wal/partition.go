package wal

import "fmt"

// PartitionStrategy determines how messages are assigned to partitions.
type PartitionStrategy int

const (
	PartitionByTable  PartitionStrategy = iota // hash by table name
	PartitionByAction                          // hash by action type
	PartitionByPK                              // hash by first column value
)

// Partitioner assigns WAL messages to a numbered partition bucket.
type Partitioner struct {
	n        int
	strategy PartitionStrategy
}

// NewPartitioner creates a Partitioner with n buckets and the given strategy.
// n must be >= 1.
func NewPartitioner(n int, strategy PartitionStrategy) (*Partitioner, error) {
	if n < 1 {
		return nil, fmt.Errorf("partitioner: n must be >= 1, got %d", n)
	}
	return &Partitioner{n: n, strategy: strategy}, nil
}

// Partition returns the zero-based partition index for msg in [0, n).
// Returns 0 for nil messages.
func (p *Partitioner) Partition(msg *Message) int {
	if msg == nil {
		return 0
	}
	var key string
	switch p.strategy {
	case PartitionByTable:
		key = msg.Table
	case PartitionByAction:
		key = msg.Action
	case PartitionByPK:
		if len(msg.Columns) > 0 {
			key = fmt.Sprintf("%v", msg.Columns[0].Value)
		}
	default:
		key = msg.Table
	}
	return fnv32(key) % p.n
}

// Buckets returns the total number of partitions.
func (p *Partitioner) Buckets() int { return p.n }

// fnv32 is a simple non-cryptographic hash of s.
func fnv32(s string) int {
	const (
		offsetBasis uint32 = 2166136261
		prime       uint32 = 16777619
	)
	h := offsetBasis
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	if h == 0 {
		return 0
	}
	v := int(h)
	if v < 0 {
		v = -v
	}
	return v
}
