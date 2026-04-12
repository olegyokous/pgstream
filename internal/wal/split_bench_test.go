package wal

import "testing"

func BenchmarkSplitter_Dispatch(b *testing.B) {
	s, _ := NewSplitter(func(m *Message) bool {
		return m.Action == "INSERT"
	}, SplitterConfig{BufferSize: b.N + 1})

	msg := &Message{Table: "bench", Action: "INSERT"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Dispatch(msg)
	}
}

func BenchmarkSplitter_DispatchParallel(b *testing.B) {
	s, _ := NewSplitter(func(m *Message) bool {
		return m.Table == "left"
	}, SplitterConfig{BufferSize: 4096})

	// Drain channels in background so they never block.
	go func() { for range s.Left {} }()
	go func() { for range s.Right {} }()

	b.RunParallel(func(pb *testing.PB) {
		msg := &Message{Table: "left", Action: "INSERT"}
		for pb.Next() {
			_ = s.Dispatch(msg)
		}
	})
}
