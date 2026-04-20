package wal

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestQuorum_ConcurrentDecideIsSafe(t *testing.T) {
	var calls int64
	v := func(m *Message) (bool, error) {
		atomic.AddInt64(&calls, 1)
		return m.Relation == "safe", nil
	}

	q, err := NewQuorum(QuorumMajority, v, v, v)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			msg := &Message{Relation: "safe", Action: "INSERT"}
			ok, err := q.Decide(msg)
			if err != nil || !ok {
				t.Errorf("expected true, got ok=%v err=%v", ok, err)
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt64(&calls) != int64(goroutines*3) {
		t.Errorf("expected %d voter calls, got %d", goroutines*3, calls)
	}
}

func TestQuorum_MixedTableDecisions(t *testing.T) {
	allowOrders := func(m *Message) (bool, error) {
		return m.Relation == "orders", nil
	}
	allowUsers := func(m *Message) (bool, error) {
		return m.Relation == "users", nil
	}

	q, _ := NewQuorum(QuorumAny, allowOrders, allowUsers)

	cases := []struct {
		table string
		want  bool
	}{
		{"orders", true},
		{"users", true},
		{"logs", false},
	}

	for _, tc := range cases {
		ok, err := q.Decide(&Message{Relation: tc.table, Action: "INSERT"})
		if err != nil {
			t.Fatalf("table %s: unexpected error: %v", tc.table, err)
		}
		if ok != tc.want {
			t.Errorf("table %s: got %v, want %v", tc.table, ok, tc.want)
		}
	}
}
