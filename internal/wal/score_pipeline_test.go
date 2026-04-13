package wal

import (
	"strings"
	"testing"
)

func TestScorer_IntegratesWithLabeler(t *testing.T) {
	// Build a scorer that scores DELETE highly.
	scoreCfg := DefaultScorerConfig()
	scoreCfg.Rules = []ScoreRule{
		{Action: "DELETE", Score: 9.9},
	}
	scorer, _ := NewScorer(scoreCfg)

	// Build a labeler that stamps env=prod.
	labelCfg := WithLabelOverwrite(false)
	labeler, _ := NewLabeler(map[string]string{"env": "prod"}, labelCfg)

	msg := scoreMsg("orders", "DELETE")
	msg = labeler.Apply(msg)
	msg = scorer.Apply(msg)

	if msg.Meta["env"] != "prod" {
		t.Fatalf("expected env=prod, got %s", msg.Meta["env"])
	}
	if msg.Meta["score"] != "9.9" {
		t.Fatalf("expected score=9.9, got %s", msg.Meta["score"])
	}
}

func TestScorer_CustomFieldName(t *testing.T) {
	cfg := DefaultScorerConfig()
	cfg.Field = "priority_score"
	cfg.DefaultScore = 3.14
	s, _ := NewScorer(cfg)

	out := s.Apply(scoreMsg("t", "INSERT"))
	val, ok := out.Meta["priority_score"]
	if !ok {
		t.Fatal("expected priority_score in meta")
	}
	if !strings.HasPrefix(val, "3.14") {
		t.Fatalf("unexpected value: %s", val)
	}
}

func TestScorer_ChainedWithTagger(t *testing.T) {
	taggerCfg := []TagRule{
		{Table: "events", Tags: map[string]string{"source": "cdc"}},
	}
	tagger, _ := NewTagger(taggerCfg)

	scoreCfg := DefaultScorerConfig()
	scoreCfg.Rules = []ScoreRule{
		{Table: "events", Score: 42.0},
	}
	scorer, _ := NewScorer(scoreCfg)

	msg := scoreMsg("events", "INSERT")
	msg = tagger.Apply(msg)
	msg = scorer.Apply(msg)

	if msg.Meta["source"] != "cdc" {
		t.Fatalf("expected source=cdc, got %s", msg.Meta["source"])
	}
	if msg.Meta["score"] != "42" {
		t.Fatalf("expected score=42, got %s", msg.Meta["score"])
	}
}
