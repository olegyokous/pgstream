package wal

import (
	"sync"
	"testing"
)

func TestScorer_ConcurrentApplyIsSafe(t *testing.T) {
	cfg := DefaultScorerConfig()
	cfg.Rules = []ScoreRule{
		{Table: "orders", Score: 7.0},
		{Action: "DELETE", Score: 2.0},
	}
	s, _ := NewScorer(cfg)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			table := "orders"
			if i%2 == 0 {
				table = "users"
			}
			msg := scoreMsg(table, "INSERT")
			out := s.Apply(msg)
			if out == nil {
				t.Errorf("unexpected nil output")
			}
		}(i)
	}
	wg.Wait()
}

func TestScorer_MultipleTablesGetCorrectScores(t *testing.T) {
	cfg := DefaultScorerConfig()
	cfg.DefaultScore = -1.0
	cfg.Rules = []ScoreRule{
		{Table: "payments", Action: "INSERT", Score: 100.0},
		{Table: "payments", Score: 50.0},
		{Table: "users", Score: 25.0},
	}
	s, _ := NewScorer(cfg)

	cases := []struct {
		table, action, want string
	}{
		{"payments", "INSERT", "100"},
		{"payments", "UPDATE", "50"},
		{"users", "DELETE", "25"},
		{"orders", "INSERT", "-1"},
	}
	for _, tc := range cases {
		out := s.Apply(scoreMsg(tc.table, tc.action))
		if out.Meta["score"] != tc.want {
			t.Errorf("table=%s action=%s: expected %s, got %s", tc.table, tc.action, tc.want, out.Meta["score"])
		}
	}
}
