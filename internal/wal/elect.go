package wal

import (
	"sync"
	"time"
)

// ElectConfig holds configuration for the leader elector.
type ElectConfig struct {
	TTL      time.Duration
	NodeID   string
}

// DefaultElectConfig returns a sensible default configuration.
func DefaultElectConfig(nodeID string) ElectConfig {
	return ElectConfig{
		TTL:    30 * time.Second,
		NodeID: nodeID,
	}
}

// Elector implements a simple in-process leader election mechanism.
// It tracks which node currently holds the lease and whether it has expired.
type Elector struct {
	mu       sync.Mutex
	cfg      ElectConfig
	leader   string
	acquired time.Time
	clock    func() time.Time
}

// NewElector creates a new Elector with the given configuration.
func NewElector(cfg ElectConfig, opts ...func(*Elector)) *Elector {
	e := &Elector{
		cfg:   cfg,
		clock: time.Now,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

func withElectorClock(fn func() time.Time) func(*Elector) {
	return func(e *Elector) { e.clock = fn }
}

// Acquire attempts to claim leadership for the configured node.
// Returns true if the node is now the leader.
func (e *Elector) Acquire() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.clock()
	if e.leader == "" || now.Sub(e.acquired) > e.cfg.TTL {
		e.leader = e.cfg.NodeID
		e.acquired = now
		return true
	}
	return e.leader == e.cfg.NodeID
}

// Renew refreshes the lease for the current leader.
// Returns false if this node is not the current leader.
func (e *Elector) Renew() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.leader != e.cfg.NodeID {
		return false
	}
	e.acquired = e.clock()
	return true
}

// Release relinquishes leadership if held by this node.
func (e *Elector) Release() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.leader == e.cfg.NodeID {
		e.leader = ""
	}
}

// Leader returns the current leader node ID and whether the lease is valid.
func (e *Elector) Leader() (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.leader == "" {
		return "", false
	}
	valid := e.clock().Sub(e.acquired) <= e.cfg.TTL
	return e.leader, valid
}
