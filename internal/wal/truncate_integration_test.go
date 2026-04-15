package wal_test

import (
	"sync"
	"testing"

	"github.com/your-org/pgstream/internal/wal"
)

func TestTruncator_ConcurrentApplyIsSafe(t *testing.T) {
	truncator, err := wal.NewTruncator(wal.WithMaxBytes(256), wal.WithTruncateAction(wal.TruncateDrop))
	if err != nil {
		t.Fatalf("NewTruncator error: %v", err)
	}

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			msg := &wal.Message{
				Table:  "events",
				Action: "INSERT",
				Columns: []wal.Column{
					{Name: "id", Value: "abc"},
				},
			}
			_, err := truncator.Apply(msg)
			if err != nil {
				t.Errorf("goroutine %d: Apply error: %v", i, err)
			}
		}(i)
	}

	wg.Wait()
}

func TestTruncator_LargePayloadIsDropped(t *testing.T) {
	const maxBytes = 64
	truncator, err := wal.NewTruncator(wal.WithMaxBytes(maxBytes), wal.WithTruncateAction(wal.TruncateDrop))
	if err != nil {
		t.Fatalf("NewTruncator error: %v", err)
	}

	// Build a payload that will definitely exceed maxBytes.
	big := make([]byte, maxBytes*4)
	for i := range big {
		big[i] = 'A'
	}

	msg := &wal.Message{
		Table:  "bulk",
		Action: "INSERT",
		Columns: []wal.Column{
			{Name: "data", Value: string(big)},
		},
	}

	out, err := truncator.Apply(msg)
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	if out != nil {
		t.Errorf("expected nil for oversized message, got table %q", out.Table)
	}
}
