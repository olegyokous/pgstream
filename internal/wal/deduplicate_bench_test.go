package wal

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkContentHasher_IsDuplicate(b *testing.B) {
	h, _ := NewContentHasher(ContentHasherConfig{
		TTL:     time.Minute,
		MaxSize: 100_000,
	})
	msgs := make([]*Message, 1000)
	for i := range msgs {
		msgs[i] = &Message{
			Table:  "bench",
			Action: "INSERT",
			Columns: []Column{
				{Name: "id", Value: fmt.Sprintf("key-%d", i)},
			},
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.IsDuplicate(msgs[i%1000])
	}
}

func BenchmarkContentHasher_IsDuplicateParallel(b *testing.B) {
	h, _ := NewContentHasher(ContentHasherConfig{
		TTL:     time.Minute,
		MaxSize: 100_000,
	})
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			msg := &Message{
				Table:  "bench",
				Action: "UPDATE",
				Columns: []Column{
					{Name: "id", Value: fmt.Sprintf("k-%d", i%500)},
				},
			}
			h.IsDuplicate(msg)
			i++
		}
	})
}
