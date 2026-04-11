package wal

import (
	"testing"
	"time"
)

func sampleMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestSampler_DefaultConfigPassesAll(t *testing.T) {
	s := NewSampler(DefaultSamplerConfig())
	for i := 0; i < 100; i++ {
		if !s.Sample(sampleMsg("t", "INSERT")) {
			t.Fatal("expected all messages to pass with rate=1.0")
		}
	}
}

func TestSampler_ZeroRateDropsAll(t *testing.T) {
	s := NewSampler(SamplerConfig{Rate: 0.0})
	for i := 0; i < 50; i++ {
		if s.Sample(sampleMsg("t", "INSERT")) {
			t.Fatal("expected all messages to be dropped with rate=0.0")
		}
	}
}

func TestSampler_RateFiltersApproximately(t *testing.T) {
	counter := 0
	// Alternate rand values: 0.3, 0.7, 0.3, 0.7 …
	toggle := false
	s := NewSampler(SamplerConfig{Rate: 0.5}, withSamplerRand(func() float64 {
		toggle = !toggle
		if toggle {
			return 0.3 // < 0.5 → pass
		}
		return 0.7 // >= 0.5 → drop
	}))
	for i := 0; i < 100; i++ {
		if s.Sample(sampleMsg("t", "INSERT")) {
			counter++
		}
	}
	if counter != 50 {
		t.Fatalf("expected 50 passes, got %d", counter)
	}
}

func TestSampler_NilMessageReturnsFalse(t *testing.T) {
	s := NewSampler(DefaultSamplerConfig())
	if s.Sample(nil) {
		t.Fatal("expected nil message to return false")
	}
}

func TestSampler_MaxPerSecondCapsMessages(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	s := NewSampler(
		SamplerConfig{Rate: 1.0, MaxPerSecond: 3},
		withSamplerClock(func() time.Time { return now }),
	)
	passed := 0
	for i := 0; i < 10; i++ {
		if s.Sample(sampleMsg("t", "INSERT")) {
			passed++
		}
	}
	if passed != 3 {
		t.Fatalf("expected 3 messages within the second, got %d", passed)
	}
}

func TestSampler_MaxPerSecondResetsOnNewSecond(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	s := NewSampler(
		SamplerConfig{Rate: 1.0, MaxPerSecond: 2},
		withSamplerClock(func() time.Time { return now }),
	)
	// Exhaust first window.
	s.Sample(sampleMsg("t", "INSERT"))
	s.Sample(sampleMsg("t", "INSERT"))
	if s.Sample(sampleMsg("t", "INSERT")) {
		t.Fatal("expected third message to be dropped in same second")
	}
	// Advance clock by 1 second.
	now = now.Add(time.Second)
	if !s.Sample(sampleMsg("t", "INSERT")) {
		t.Fatal("expected message to pass after window reset")
	}
}
