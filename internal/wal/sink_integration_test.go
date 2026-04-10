package wal

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestSink_ConcurrentFileWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent.log")

	s, err := NewSink(SinkFile, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	const workers = 10
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_ = s.Write(context.Background(), []byte("concurrent-line"))
		}()
	}
	wg.Wait()
	s.Close()

	data, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != workers {
		t.Errorf("expected %d lines, got %d", workers, len(lines))
	}
}

func TestSink_AppendToExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "append.log")

	for i := 0; i < 3; i++ {
		s, err := NewSink(SinkFile, path)
		if err != nil {
			t.Fatalf("open attempt %d: %v", i, err)
		}
		_ = s.Write(context.Background(), []byte("line"))
		s.Close()
	}

	data, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 appended lines, got %d", len(lines))
	}
}
