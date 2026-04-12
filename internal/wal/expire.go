package wal

import (
	"errors"
	"time"
)

// DefaultExpirerConfig returns a sensible default configuration.
func DefaultExpirerConfig() ExpirerConfig {
	return ExpirerConfig{
		TTL: 5 * time.Minute,
	}
}

// ExpirerConfig controls message expiry behaviour.
type ExpirerConfig struct {
	// TTL is the maximum age a message may have before it is considered expired.
	TTL time.Duration
	// Table restricts expiry checks to a specific table name (empty = all tables).
	Table string
}

// Expirer drops messages whose WalTimestamp is older than the configured TTL.
type Expirer struct {
	cfg   ExpirerConfig
	clock func() time.Time
}

// NewExpirer constructs an Expirer with the given config.
func NewExpirer(cfg ExpirerConfig, opts ...func(*Expirer)) (*Expirer, error) {
	if cfg.TTL <= 0 {
		return nil, errors.New("expirer: TTL must be positive")
	}
	e := &Expirer{cfg: cfg, clock: time.Now}
	for _, o := range opts {
		o(e)
	}
	return e, nil
}

func withExpirerClock(fn func() time.Time) func(*Expirer) {
	return func(e *Expirer) { e.clock = fn }
}

// Apply returns nil if the message has expired, otherwise it returns the
// message unchanged.
func (e *Expirer) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	if e.cfg.Table != "" && msg.Table != e.cfg.Table {
		return msg
	}
	if msg.WalTimestamp.IsZero() {
		return msg
	}
	if e.clock().Sub(msg.WalTimestamp) > e.cfg.TTL {
		return nil
	}
	return msg
}
