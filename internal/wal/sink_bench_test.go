package wal

import (
	"context"
	"path/filepath"
	"testing"
)

func BenchmarkSink_NullWrite(b *testing.B) {
	s, _ := NewSink(SinkNull, "")
	defer s.Close()
	payload := []byte(`{"table":"orders","action":"INSERT"}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Write(context.Background(), payload)
	}
}

func BenchmarkSink_FileWrite(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.log")
	s, err := NewSink(SinkFile, path)
	if err != nil {
		b.Fatalf("sink: %v", err)
	}
	defer s.Close()
	payload := []byte(`{"table":"orders","action":"INSERT","columns":{"id":"1"}}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Write(context.Background(), payload)
	}
}
