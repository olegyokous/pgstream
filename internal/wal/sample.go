package wal

import "time"

// SamplerConfig controls probabilistic and rate-based sampling of WAL messages.
type SamplerConfig struct {
	// Rate is the fraction of messages to pass through (0.0–1.0).
	// A value of 1.0 means all messages pass; 0.0 drops all.
	Rate float64
	// MaxPerSecond, if > 0, caps the number of messages emitted per second
	// regardless of Rate.
	MaxPerSecond int
}

// DefaultSamplerConfig returns a SamplerConfig that passes all messages.
func DefaultSamplerConfig() SamplerConfig {
	return SamplerConfig{
		Rate:         1.0,
		MaxPerSecond: 0,
	}
}

// Sampler probabilistically drops WAL messages based on a configured rate.
type Sampler struct {
	cfg     SamplerConfig
	clock   func() time.Time
	rand    func() float64
	bucket  int       // messages emitted in current second window
	window  time.Time // start of current second window
}

// NewSampler constructs a Sampler with the given config.
// Passing a zero-value config is equivalent to DefaultSamplerConfig.
func NewSampler(cfg SamplerConfig, opts ...func(*Sampler)) *Sampler {
	if cfg.Rate <= 0 && cfg.Rate != 0 {
		cfg.Rate = 0
	}
	if cfg.Rate > 1.0 {
		cfg.Rate = 1.0
	}
	s := &Sampler{
		cfg:   cfg,
		clock: time.Now,
		rand:  defaultRandFloat64,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// withSamplerClock overrides the clock (for testing).
func withSamplerClock(fn func() time.Time) func(*Sampler) {
	return func(s *Sampler) { s.clock = fn }
}

// withSamplerRand overrides the random source (for testing).
func withSamplerRand(fn func() float64) func(*Sampler) {
	return func(s *Sampler) { s.rand = fn }
}

// Sample returns true if the message should be forwarded.
func (s *Sampler) Sample(msg *Message) bool {
	if msg == nil {
		return false
	}
	if s.cfg.Rate < 1.0 && s.rand() >= s.cfg.Rate {
		return false
	}
	if s.cfg.MaxPerSecond > 0 {
		now := s.clock()
		if now.Sub(s.window) >= time.Second {
			s.window = now
			s.bucket = 0
		}
		if s.bucket >= s.cfg.MaxPerSecond {
			return false
		}
		s.bucket++
	}
	return true
}
