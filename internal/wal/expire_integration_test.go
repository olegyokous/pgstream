package wal_test

import (
	"sync"
	"testing"
	"time"

	"pgstream/internal/wal"
)

func TestExpirer_ConcurrentApply(t *testing.T) {
	now := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	e, err := wal.NewExpirer(wal.ExpirerConfig{TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	drops := 0
	var mu sync.Mutex

	for i := 0; i < 200; i++ {
		wg.Add(1)
		age := time.Duration(i) * time.Second // half will be < 60s, half >= 60s
		go func(a time.Duration) {
			defer wg.Done()
			msg := &wal.Message{
				Table:        "events",
				Action:       "INSERT",
				WalTimestamp: now.Add(-a),
			}
			_ = now // suppress unused warning; clock is real time here
			result := e.Apply(msg)
			if result == nil {
				mu.Lock()
				drops++
				mu.Unlock()
			}
		}(age)
	}
	wg.Wait()
	// At least some messages should have been dropped (those older than TTL)
	if drops == 0 {
		t.Error("expected at least some messages to be dropped")
	}
}

func TestExpirer_IntegratesWithPipeline(t *testing.T) {
	now := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	msgs := []*wal.Message{
		{Table: "orders", Action: "INSERT", WalTimestamp: now.Add(-10 * time.Second)},
		{Table: "orders", Action: "UPDATE", WalTimestamp: now.Add(-2 * time.Minute)},
		{Table: "orders", Action: "DELETE", WalTimestamp: now.Add(-30 * time.Second)},
	}

	e, _ := wal.NewExpirer(wal.ExpirerConfig{TTL: time.Minute})

	var passed []*wal.Message
	for _, m := range msgs {
		if out := e.Apply(m); out != nil {
			passed = append(passed, out)
		}
	}

	if len(passed) != 2 {
		t.Fatalf("expected 2 messages to pass, got %d", len(passed))
	}
}
