package wal

import "testing"

func BenchmarkPinner_IsPinned(b *testing.B) {
	p, _ := NewPinner()
	p.Pin("orders")
	m := &Message{Table: "orders", Action: "INSERT"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.IsPinned(m)
	}
}

func BenchmarkPinner_IsPinnedParallel(b *testing.B) {
	p, _ := NewPinner()
	p.Pin("orders")
	m := &Message{Table: "orders", Action: "INSERT"}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.IsPinned(m)
		}
	})
}
