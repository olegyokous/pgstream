package wal

import "testing"

func BenchmarkForwarder_NoMatch(b *testing.B) {
	f, _ := NewForwarder(
		func(m *Message) bool { return m.Relation == "never" },
		WithForwardTarget(func(*Message) error { return nil }),
	)
	msg := forwardMsg("orders", "INSERT")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = f.Apply(msg)
	}
}

func BenchmarkForwarder_Match(b *testing.B) {
	f, _ := NewForwarder(
		func(*Message) bool { return true },
		WithForwardTarget(func(*Message) error { return nil }),
	)
	msg := forwardMsg("orders", "INSERT")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = f.Apply(msg)
	}
}

func BenchmarkForwarder_MatchParallel(b *testing.B) {
	f, _ := NewForwarder(
		func(*Message) bool { return true },
		WithForwardTarget(func(*Message) error { return nil }),
	)
	msg := forwardMsg("orders", "INSERT")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = f.Apply(msg)
		}
	})
}
