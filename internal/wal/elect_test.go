package wal

import (
	"testing"
	"time"
)

func TestElector_DefaultConfigApplied(t *testing.T) {
	cfg := DefaultElectConfig("node-1")
	if cfg.NodeID != "node-1" {
		t.Fatalf("expected node-1, got %s", cfg.NodeID)
	}
	if cfg.TTL != 30*time.Second {
		t.Fatalf("expected 30s TTL, got %v", cfg.TTL)
	}
}

func TestElector_AcquireGrantsLeadership(t *testing.T) {
	e := NewElector(DefaultElectConfig("node-1"))
	if !e.Acquire() {
		t.Fatal("expected Acquire to succeed on empty elector")
	}
	leader, ok := e.Leader()
	if !ok || leader != "node-1" {
		t.Fatalf("expected leader node-1, got %s ok=%v", leader, ok)
	}
}

func TestElector_SecondNodeCannotAcquire(t *testing.T) {
	e1 := NewElector(DefaultElectConfig("node-1"))
	e1.Acquire()

	e2cfg := DefaultElectConfig("node-2")
	e2 := &Elector{cfg: e2cfg, clock: e1.clock, mu: e1.mu}
	// share same elector state by pointing at same fields via a fresh elector
	e := NewElector(e1.cfg)
	e.Acquire()
	e2 := NewElector(e2cfg)
	// simulate same shared state: copy internal fields
	e2.leader = e.leader
	e2.acquired = e.acquired
	if e2.Acquire() {
		t.Fatal("second node should not acquire while first holds lease")
	}
}

func TestElector_ExpiredLeaseAllowsNewAcquire(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }
	cfg := ElectConfig{TTL: 5 * time.Second, NodeID: "node-1"}
	e := NewElector(cfg, withElectorClock(clock))
	e.Acquire()

	// advance past TTL
	now = now.Add(10 * time.Second)
	cfg2 := ElectConfig{TTL: 5 * time.Second, NodeID: "node-2"}
	e2 := NewElector(cfg2, withElectorClock(clock))
	// share state
	e2.leader = e.leader
	e2.acquired = e.acquired
	if !e2.Acquire() {
		t.Fatal("expected node-2 to acquire after TTL expiry")
	}
}

func TestElector_RenewExtendsTTL(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }
	cfg := ElectConfig{TTL: 5 * time.Second, NodeID: "node-1"}
	e := NewElector(cfg, withElectorClock(clock))
	e.Acquire()

	now = now.Add(4 * time.Second)
	if !e.Renew() {
		t.Fatal("expected Renew to succeed for current leader")
	}
	now = now.Add(4 * time.Second)
	_, ok := e.Leader()
	if !ok {
		t.Fatal("expected lease to still be valid after renew")
	}
}

func TestElector_ReleaseDropsLeadership(t *testing.T) {
	e := NewElector(DefaultElectConfig("node-1"))
	e.Acquire()
	e.Release()
	leader, ok := e.Leader()
	if ok || leader != "" {
		t.Fatalf("expected no leader after release, got %s", leader)
	}
}

func TestElector_LeaderReturnsFalseWhenExpired(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }
	cfg := ElectConfig{TTL: 1 * time.Second, NodeID: "node-1"}
	e := NewElector(cfg, withElectorClock(clock))
	e.Acquire()
	now = now.Add(2 * time.Second)
	_, ok := e.Leader()
	if ok {
		t.Fatal("expected leader to be invalid after TTL")
	}
}
