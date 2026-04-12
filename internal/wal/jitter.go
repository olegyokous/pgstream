package wal

import (
	"math/rand"
	"time"
)

// JitterConfig controls how random jitter is applied to durations.
type JitterConfig struct {
	// Factor is the maximum fractional jitter to apply (0.0–1.0).
	// A factor of 0.2 means up to ±20% of the base duration is added.
	Factor float64
}

// DefaultJitterConfig returns a JitterConfig with sensible defaults.
func DefaultJitterConfig() JitterConfig {
	return JitterConfig{
		Factor: 0.2,
	}
}

// Jitterer applies random jitter to durations.
type Jitterer struct {
	cfg  JitterConfig
	rand func() float64
}

// NewJitterer creates a Jitterer with the given config.
// If cfg.Factor is outside (0, 1], it is clamped to 0.2.
func NewJitterer(cfg JitterConfig) *Jitterer {
	if cfg.Factor <= 0 || cfg.Factor > 1 {
		cfg.Factor = 0.2
	}
	return &Jitterer{
		cfg:  cfg,
		rand: rand.Float64,
	}
}

// withJittererRand replaces the random source (for testing).
func withJittererRand(j *Jitterer, fn func() float64) *Jitterer {
	j.rand = fn
	return j
}

// Apply returns d with a random jitter in the range [-factor*d, +factor*d].
// The result is never negative.
func (j *Jitterer) Apply(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	// rand in [0, 1) → shift to [-0.5, 0.5) → scale by 2*factor
	offset := (j.rand() - 0.5) * 2 * j.cfg.Factor
	jittered := d + time.Duration(float64(d)*offset)
	if jittered < 0 {
		return 0
	}
	return jittered
}

// ApplyPositive is like Apply but always returns at least minFloor.
func (j *Jitterer) ApplyPositive(d time.Duration, minFloor time.Duration) time.Duration {
	result := j.Apply(d)
	if result < minFloor {
		return minFloor
	}
	return result
}
