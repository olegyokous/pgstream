package wal

import (
	"testing"
)

func scoreMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestNewScorer_EmptyFieldErrors(t *testing.T) {
	_, err := NewScorer(ScorerConfig{Field: ""})
	if err == nil {
		t.Fatal("expected error for empty field")
	}
}

func TestNewScorer_ValidConfig(t *testing.T) {
	s, err := NewScorer(DefaultScorerConfig())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil scorer")
	}
}

func TestScorer_NilMessagePassthrough(t *testing.T) {
	s, _ := NewScorer(DefaultScorerConfig())
	if s.Apply(nil) != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestScorer_DefaultScoreWhenNoRuleMatches(t *testing.T) {
	cfg := DefaultScorerConfig()
	cfg.DefaultScore = 1.5
	s, _ := NewScorer(cfg)
	msg := scoreMsg("orders", "INSERT")
	out := s.Apply(msg)
	if out.Meta["score"] != "1.5" {
		t.Fatalf("expected 1.5, got %s", out.Meta["score"])
	}
}

func TestScorer_TableRuleMatchesCorrectly(t *testing.T) {
	cfg := DefaultScorerConfig()
	cfg.Rules = []ScoreRule{
		{Table: "payments", Score: 10.0},
	}
	s, _ := NewScorer(cfg)

	match := s.Apply(scoreMsg("payments", "INSERT"))
	if match.Meta["score"] != "10" {
		t.Fatalf("expected 10, got %s", match.Meta["score"])
	}

	nomatch := s.Apply(scoreMsg("users", "INSERT"))
	if nomatch.Meta["score"] != "0" {
		t.Fatalf("expected 0, got %s", nomatch.Meta["score"])
	}
}

func TestScorer_ActionRuleMatchesCorrectly(t *testing.T) {
	cfg := DefaultScorerConfig()
	cfg.Rules = []ScoreRule{
		{Action: "DELETE", Score: 5.0},
	}
	s, _ := NewScorer(cfg)
	out := s.Apply(scoreMsg("any", "DELETE"))
	if out.Meta["score"] != "5" {
		t.Fatalf("expected 5, got %s", out.Meta["score"])
	}
}

func TestScorer_FirstMatchingRuleWins(t *testing.T) {
	cfg := DefaultScorerConfig()
	cfg.Rules = []ScoreRule{
		{Table: "orders", Score: 3.0},
		{Table: "orders", Score: 99.0},
	}
	s, _ := NewScorer(cfg)
	out := s.Apply(scoreMsg("orders", "UPDATE"))
	if out.Meta["score"] != "3" {
		t.Fatalf("expected 3, got %s", out.Meta["score"])
	}
}

func TestScorer_InitialisesNilMeta(t *testing.T) {
	s, _ := NewScorer(DefaultScorerConfig())
	msg := &Message{Table: "t", Action: "INSERT"}
	msg.Meta = nil
	out := s.Apply(msg)
	if out.Meta == nil {
		t.Fatal("expected meta to be initialised")
	}
}
