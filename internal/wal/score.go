package wal

import "fmt"

// ScoreRule assigns a numeric score to a message based on table and/or action.
type ScoreRule struct {
	Table  string
	Action string
	Score  float64
}

// ScorerConfig holds configuration for the Scorer.
type ScorerConfig struct {
	Rules        []ScoreRule
	DefaultScore float64
	Field        string // meta field name to store the score
}

// DefaultScorerConfig returns a ScorerConfig with sensible defaults.
func DefaultScorerConfig() ScorerConfig {
	return ScorerConfig{
		DefaultScore: 0.0,
		Field:        "score",
	}
}

// Scorer assigns a numeric score to each WAL message based on configurable rules.
type Scorer struct {
	cfg ScorerConfig
}

// NewScorer creates a new Scorer. Returns an error if no field name is provided.
func NewScorer(cfg ScorerConfig) (*Scorer, error) {
	if cfg.Field == "" {
		return nil, fmt.Errorf("scorer: field name must not be empty")
	}
	return &Scorer{cfg: cfg}, nil
}

// Apply assigns a score to msg by evaluating rules in order; the first match wins.
// If no rule matches, the default score is used. Returns nil for nil input.
func (s *Scorer) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	score := s.cfg.DefaultScore
	for _, r := range s.cfg.Rules {
		tableMatch := r.Table == "" || r.Table == msg.Table
		actionMatch := r.Action == "" || r.Action == msg.Action
		if tableMatch && actionMatch {
			score = r.Score
			break
		}
	}
	if msg.Meta == nil {
		msg.Meta = make(map[string]string)
	}
	msg.Meta[s.cfg.Field] = fmt.Sprintf("%g", score)
	return msg
}
