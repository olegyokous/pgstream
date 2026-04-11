package wal

import (
	"sync"
	"testing"
	"time"
)

func TestWindow_ConcurrentRecords(t *testing.T) {
	w, err := NewWindow(WindowConfig{Size: 10, Interval: time.Second})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	const goroutines = 50
	const recordsEach = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < recordsEach; j++ {
				w.Record(1)
			}
		}()
	}
	wg.Wait()

	got := w.Count()
	expected := int64(goroutines * recordsEach)
	if got != expected {
		t.Fatalf("expected %d, got %d", expected, got)
	}
}

func TestWindow_RollingCountOverTime(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timing-sensitive test in short mode")
	}

	w, _ := NewWindow(WindowConfig{Size: 3, Interval: 50 * time.Millisecond})

	w.Record(10)
	time.Sleep(60 * time.Millisecond)
	w.Record(5)

	got := w.Count()
	if got != 15 {
		t.Fatalf("expected 15 within window, got %d", got)
	}

	// Wait for first record to expire (3 * 50ms = 150ms from start)
	time.Sleep(120 * time.Millisecond)
	w.Record(0) // trigger advance

	got = w.Count()
	if got != 5 {
		t.Fatalf("expected 5 after first record expires, got %d", got)
	}
}
