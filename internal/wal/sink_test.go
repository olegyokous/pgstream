package wal

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewSink_UnknownType(t *testing.T) {
	_, err := NewSink("kafka", "")
	if err == nil {
		t.Fatal("expected error for unknown sink type")
	}
}

func TestNewSink_FileRequiresTarget(t *testing.T) {
	_, err := NewSink(SinkFile, "")
	if err == nil {
		t.Fatal("expected error when file target is empty")
	}
}

func TestNewSink_NullDiscards(t *testing.T) {
	s, err := NewSink(SinkNull, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()
	if err := s.Write(context.Background(), []byte("hello")); err != nil {
		t.Fatalf("null sink write error: %v", err)
	}
}

func TestNewSink_StdoutDoesNotError(t *testing.T) {
	s, err := NewSink(SinkStdout, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()
	if err := s.Write(context.Background(), []byte("ping")); err != nil {
		t.Fatalf("stdout sink write error: %v", err)
	}
}

func TestNewSink_FileWritesContent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.log")

	s, err := NewSink(SinkFile, path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := s.Write(context.Background(), []byte("row1")); err != nil {
		t.Fatalf("write error: %v", err)
	}
	if err := s.Write(context.Background(), []byte("row2")); err != nil {
		t.Fatalf("write error: %v", err)
	}
	s.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	got := string(data)
	if !strings.Contains(got, "row1") || !strings.Contains(got, "row2") {
		t.Errorf("file content missing expected rows, got: %q", got)
	}
}
