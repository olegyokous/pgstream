package wal

import (
	"testing"
	"time"
)

// TestElector_LeaderGatesMessageProcessing verifies that an elector can be
// used to gate message processing: only the leader node should handle msgs.
func TestElector_LeaderGatesMessageProcessing(t *testing.T) {
	cfg := ElectConfig{TTL: 10 * time.Second, NodeID: "node-leader"}
	leader := NewElector(cfg)
	leader.Acquire()

	standbyCfg := ElectConfig{TTL: 10 * time.Second, NodeID: "node-standby"}
	standby := NewElector(standbyCfg)

	msg := &Message{
		Table:  "orders",
		Action: "INSERT",
		Columns: []Column{
			{Name: "id", Value: "42"},
		},
	}

	processed := 0
	process := func(e *Elector, m *Message) bool {
		_, ok := e.Leader()
		if !ok {
			return false
		}
		processed++
		return true
	}

	if !process(leader, msg) {
		t.Fatal("leader should process message")
	}
	if process(standby, msg) {
		t.Fatal("standby should not process message")
	}
	if processed != 1 {
		t.Fatalf("expected 1 processed, got %d", processed)
	}
}

// TestElector_FailoverAllowsStandbyToProcess verifies that after the leader
// releases, a standby node can acquire and process messages.
func TestElector_FailoverAllowsStandbyToProcess(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }

	leaderCfg := ElectConfig{TTL: 5 * time.Second, NodeID: "node-1"}
	leader := NewElector(leaderCfg, withElectorClock(clock))
	leader.Acquire()
	leader.Release()

	standbyCfg := ElectConfig{TTL: 5 * time.Second, NodeID: "node-2"}
	standby := NewElector(standbyCfg, withElectorClock(clock))
	if !standby.Acquire() {
		t.Fatal("standby should acquire after leader releases")
	}

	_, ok := standby.Leader()
	if !ok {
		t.Fatal("standby should now be valid leader")
	}
}
