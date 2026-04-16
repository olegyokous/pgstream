package wal

import "testing"

func BenchmarkEvolver_Apply(b *testing.B) {
	e, _ := NewEvolver(func(_ string, _ int, _ *Message) error { return nil })
	msg := evolveMsg("users")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Apply(msg) //nolint
	}
}

func BenchmarkEvolver_ApplyParallel(b *testing.B) {
	e, _ := NewEvolver(func(_ string, _ int, _ *Message) error { return nil })
	msg := evolveMsg("users")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.Apply(msg) //nolint
		}
	})
}
