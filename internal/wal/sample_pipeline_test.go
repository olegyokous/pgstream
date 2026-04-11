package wal

import (
	"testing"
	"time"
)

func TestSampler_PipelineDropsHalf(t *testing.T) {
	toggle := false
	s := NewSampler(SamplerConfig{Rate: 0.5}, withSamplerRand(func() float64 {
		toggle = !toggle
		if toggle {
			return 0.2
		}
		return 0.8
	}))

	input := make([]*Message, 20)
	for i := range input {
		input[i] = &Message{Table: "events", Action: "INSERT"}
	}

	var passed []*Message
	for _, m := range input {
		if s.Sample(m) {
			passed = append(passed, m)
		}
	}
	if len(passed) != 10 {
		t.Fatalf("expected 10 passed messages, got %d", len(passed))
	}
}

func TestSampler_MaxPerSecondInPipeline(t *testing.T) {
	now := time.Unix(500_000, 0)
	s := NewSampler(
		SamplerConfig{Rate: 1.0, MaxPerSecond: 5},
		withSamplerClock(func() time.Time { return now }),
	)

	var passed int
	for i := 0; i < 20; i++ {
		if s.Sample(&Message{Table: "logs", Action: "INSERT"}) {
			passed++
		}
	}
	if passed != 5 {
		t.Fatalf("expected 5 passed messages within window, got %d", passed)
	}

	// Advance to next second — should allow 5 more.
	now = now.Add(time.Second)
	passed = 0
	for i := 0; i < 20; i++ {
		if s.Sample(&Message{Table: "logs", Action: "INSERT"}) {
			passed++
		}
	}
	if passed != 5 {
		t.Fatalf("expected 5 passed messages in new window, got %d", passed)
	}
}
