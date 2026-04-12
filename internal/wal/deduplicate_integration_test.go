package wal_test

import (
	"sync"
	"testing"
	"time"

	"github.com/your-org/pgstream/internal/wal"
)

func TestContentHasher_ConcurrentIsDuplicate(t *testing.T) {
	h, err := wal.NewContentHasher(wal.ContentHasherConfig{
		TTL:     time.Minute,
		MaxSize: 1000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wg sync.WaitGroup
	duplicates := make([]bool, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msg := &wal.Message{
				Table:  "events",
				Action: "INSERT",
				Columns: []wal.Column{
					{Name: "id", Value: idx % 10}, // 10 unique fingerprints
				},
			}
			duplicates[idx] = h.IsDuplicate(msg)
		}(i)
	}
	wg.Wait()

	// At least 10 messages should be non-duplicate (first occurrence of each)
	non := 0
	for _, d := range duplicates {
		if !d {
			non++
		}
	}
	if non < 10 {
		t.Fatalf("expected at least 10 non-duplicates, got %d", non)
	}
}

func TestContentHasher_TTLExpiry_IntegrationRace(t *testing.T) {
	h, _ := wal.NewContentHasher(wal.ContentHasherConfig{
		TTL:     50 * time.Millisecond,
		MaxSize: 500,
	})

	msg := &wal.Message{
		Table:  "sessions",
		Action: "DELETE",
		Columns: []wal.Column{{Name: "token", Value: "abc"}},
	}

	if h.IsDuplicate(msg) {
		t.Fatal("first call should not be duplicate")
	}
	if !h.IsDuplicate(msg) {
		t.Fatal("immediate second call should be duplicate")
	}

	time.Sleep(100 * time.Millisecond)

	if h.IsDuplicate(msg) {
		t.Fatal("after TTL expiry should not be duplicate")
	}
}
