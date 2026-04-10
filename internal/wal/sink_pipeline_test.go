package wal

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSink_IntegratesWithFormatter verifies that a Sink can consume output
// produced by a Formatter, completing the format→sink leg of the pipeline.
func TestSink_IntegratesWithFormatter(t *testing.T) {
	fmt, err := NewFormatter("json")
	if err != nil {
		t.Fatalf("formatter: %v", err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "pipeline.log")
	sink, err := NewSink(SinkFile, path)
	if err != nil {
		t.Fatalf("sink: %v", err)
	}
	defer sink.Close()

	msg := sampleMsg() // reuse helper from formatter_test.go
	payload, err := fmt.Format(msg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}

	if err := sink.Write(context.Background(), payload); err != nil {
		t.Fatalf("sink write: %v", err)
	}
	sink.Close()

	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "users") {
		t.Errorf("expected table name in output, got: %s", data)
	}
}

// TestSink_NullSinkWithTextFormatter ensures the null sink works with any
// formatter without panicking.
func TestSink_NullSinkWithTextFormatter(t *testing.T) {
	fmt, err := NewFormatter("text")
	if err != nil {
		t.Fatalf("formatter: %v", err)
	}

	sink, _ := NewSink(SinkNull, "")
	defer sink.Close()

	msg := sampleMsg()
	payload, err := fmt.Format(msg)
	if err != nil {
		t.Fatalf("format: %v", err)
	}
	if err := sink.Write(context.Background(), payload); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
