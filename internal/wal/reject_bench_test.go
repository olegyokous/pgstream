package wal

import "testing"

func BenchmarkRejecter_Apply(b *testing.B) {
	r, _ := NewRejecter([]RejectRule{
		{Table: "blocked", Action: "DELETE", Reason: "bench"},
	})
	msg := rejectMsg("allowed", "INSERT")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Apply(msg)
	}
}

func BenchmarkRejecter_ApplyParallel(b *testing.B) {
	r, _ := NewRejecter([]RejectRule{
		{Table: "blocked", Action: "DELETE", Reason: "bench"},
	})
	msg := rejectMsg("allowed", "INSERT")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = r.Apply(msg)
		}
	})
}
