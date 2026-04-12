package wal

import (
	"context"
	"time"
)

// HedgeConfig controls hedged-request behaviour.
type HedgeConfig struct {
	// Delay is how long to wait before launching the hedge.
	Delay time.Duration
	// MaxHedges is the maximum number of parallel hedge attempts (including the
	// original). Must be >= 2.
	MaxHedges int
}

// DefaultHedgeConfig returns a sensible default HedgeConfig.
func DefaultHedgeConfig() HedgeConfig {
	return HedgeConfig{
		Delay:     50 * time.Millisecond,
		MaxHedges: 2,
	}
}

// Hedger issues a hedged call: it fires the primary attempt and, after Delay,
// fires up to MaxHedges-1 additional attempts. The first successful result
// wins; all others are cancelled via their context.
type Hedger struct {
	cfg HedgeConfig
}

// NewHedger constructs a Hedger. If cfg.MaxHedges < 2 it is clamped to 2.
func NewHedger(cfg HedgeConfig) *Hedger {
	if cfg.MaxHedges < 2 {
		cfg.MaxHedges = 2
	}
	if cfg.Delay <= 0 {
		cfg.Delay = DefaultHedgeConfig().Delay
	}
	return &Hedger{cfg: cfg}
}

type hedgeResult struct {
	val []byte
	err error
}

// Do calls fn up to MaxHedges times with independent cancellable contexts
// derived from parent. It returns the first non-error result, or the last
// error if all attempts fail.
func (h *Hedger) Do(parent context.Context, fn func(ctx context.Context) ([]byte, error)) ([]byte, error) {
	results := make(chan hedgeResult, h.cfg.MaxHedges)
	ctxs := make([]context.CancelFunc, 0, h.cfg.MaxHedges)

	launch := func() {
		ctx, cancel := context.WithCancel(parent)
		ctxs = append(ctxs, cancel)
		go func() {
			v, err := fn(ctx)
			results <- hedgeResult{val: v, err: err}
		}()
	}

	launch()

	ticker := time.NewTicker(h.cfg.Delay)
	defer ticker.Stop()

	var last error
	received := 0
	launched := 1

	for received < launched || launched < h.cfg.MaxHedges {
		select {
		case <-parent.Done():
			for _, c := range ctxs {
				c()
			}
			return nil, parent.Err()
		case <-ticker.C:
			if launched < h.cfg.MaxHedges {
				launch()
				launched++
			}
		case r := <-results:
			received++
			if r.err == nil {
				for _, c := range ctxs {
					c()
				}
				return r.val, nil
			}
			last = r.err
			if received == launched && launched == h.cfg.MaxHedges {
				return nil, last
			}
		}
	}
	return nil, last
}
