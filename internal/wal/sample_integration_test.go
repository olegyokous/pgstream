package wal_test

import (
	"testing"
	"time"

	"github.com/your-org/pgstream/internal/wal"
)

func TestSampler_IntegratesWithMessages(t *testing.T) {
	msgs := []*wal.Message{
		{Table: "orders", Action: "INSERT"},
		{Table: "orders", Action: "UPDATE"},
		{Table: "orders", Action: "DELETE"},
	}

	s := wal.NewSampler(wal.SamplerConfig{Rate: 1.0})
	var out []*wal.Message
	for _, m := range msgs {
		if s.Sample(m) {
			out = append(out, m)
		}
	}
	if len(out) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(out))
	}
}

func TestSampler_ConcurrentSamplesAreSafe(t *testing.T) {
	// Sampler with MaxPerSecond=0 (no window state) is safe for concurrent use
	// when only the rate path is exercised.
	s := wal.NewSampler(wal.SamplerConfig{Rate: 1.0})
	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				s.Sample(&wal.Message{Table: "t", Action: "INSERT"})
			}
			done <- struct{}{}
		}()
	}
	timeout := time.After(3 * time.Second)
	for i := 0; i < 20; i++ {
		select {
		case <-done:
		case <-timeout:
			t.Fatal("timed out waiting for goroutines")
		}
	}
}
