package wal

import "testing"

func BenchmarkScorer_Apply(b *testing.B) {
	cfg := DefaultScorerConfig()
	cfg.Rules = []ScoreRule{
		{Table: "orders", Action: "INSERT", Score: 10.0},
		{Table: "payments", Score: 5.0},
		{Action: "DELETE", Score: 1.0},
	}
	s, _ := NewScorer(cfg)
	msg := scoreMsg("orders", "INSERT")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Apply(msg)
	}
}

func BenchmarkScorer_ApplyParallel(b *testing.B) {
	cfg := DefaultScorerConfig()
	cfg.Rules = []ScoreRule{
		{Table: "orders", Score: 10.0},
	}
	s, _ := NewScorer(cfg)
	b.RunParallel(func(pb *testing.PB) {
		msg := scoreMsg("orders", "UPDATE")
		for pb.Next() {
			s.Apply(msg)
		}
	})
}
