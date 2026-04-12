package wal

import (
	"testing"
	"time"
)

func TestNewJitterer_DefaultConfigApplied(t *testing.T) {
	j := NewJitterer(JitterConfig{})
	if j.cfg.Factor != 0.2 {
		t.Fatalf("expected factor 0.2, got %v", j.cfg.Factor)
	}
}

func TestNewJitterer_ValidFactor(t *testing.T) {
	j := NewJitterer(JitterConfig{Factor: 0.5})
	if j.cfg.Factor != 0.5 {
		t.Fatalf("expected factor 0.5, got %v", j.cfg.Factor)
	}
}

func TestJitterer_ZeroDurationReturnsZero(t *testing.T) {
	j := NewJitterer(DefaultJitterConfig())
	if got := j.Apply(0); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
}

func TestJitterer_NegativeDurationPassthrough(t *testing.T) {
	j := NewJitterer(DefaultJitterConfig())
	d := -1 * time.Second
	if got := j.Apply(d); got != d {
		t.Fatalf("expected %v, got %v", d, got)
	}
}

func TestJitterer_ApplyStaysWithinBounds(t *testing.T) {
	cfg := JitterConfig{Factor: 0.3}
	j := NewJitterer(cfg)
	base := 1 * time.Second
	low := time.Duration(float64(base) * (1 - cfg.Factor))
	high := time.Duration(float64(base) * (1 + cfg.Factor))
	for i := 0; i < 200; i++ {
		got := j.Apply(base)
		if got < low || got > high {
			t.Fatalf("jittered value %v out of [%v, %v]", got, low, high)
		}
	}
}

func TestJitterer_FixedRandProducesExpected(t *testing.T) {
	j := NewJitterer(JitterConfig{Factor: 0.2})
	// rand()=1.0 → offset = (1.0-0.5)*2*0.2 = 0.2 → result = base * 1.2
	withJittererRand(j, func() float64 { return 1.0 })
	base := time.Second
	want := time.Duration(float64(base) * 1.2)
	if got := j.Apply(base); got != want {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestJitterer_ApplyPositiveEnforcesFloor(t *testing.T) {
	j := NewJitterer(JitterConfig{Factor: 1.0})
	// With factor=1.0 and rand()=0.0, offset = -1.0, result could be negative
	withJittererRand(j, func() float64 { return 0.0 })
	floor := 5 * time.Millisecond
	got := j.ApplyPositive(10*time.Millisecond, floor)
	if got < floor {
		t.Fatalf("expected at least %v, got %v", floor, got)
	}
}
