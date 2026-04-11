package wal

import "math/rand"

// defaultRandFloat64 returns a pseudo-random float64 in [0.0, 1.0).
// It is the default random source used by Sampler.
func defaultRandFloat64() float64 {
	return rand.Float64() //nolint:gosec // non-crypto sampling is intentional
}
