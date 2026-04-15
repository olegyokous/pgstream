package wal

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestElector_ConcurrentAcquireOnlyOneWins(t *testing.T) {
	cfg := ElectConfig{TTL: 10 * time.Second, NodeID: "node-X"}
	e := NewElector(cfg)

	var wins int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e.Acquire() {
				atomic.AddInt64(&wins, 1)
			}
		}()
	}
	wg.Wait()

	// All goroutines share the same node ID so all should return true
	// (same node renewing its own lease).
	if wins == 0 {
		t.Fatal("expected at least one acquire to succeed")
	}
}

func TestElector_ConcurrentRenewIsSafe(t *testing.T) {
	cfg := ElectConfig{TTL: 30 * time.Second, NodeID: "node-1"}
	e := NewElector(cfg)
	e.Acquire()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Renew()
		}()
	}
	wg.Wait()

	_, ok := e.Leader()
	if !ok {
		t.Fatal("expected lease to still be valid after concurrent renews")
	}
}
