package wal

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestPipeline_WritesFormattedMessage(t *testing.T) {
	var buf bytes.Buffer
	p, err := NewPipeline(PipelineConfig{
		Filter: FilterConfig{},
		Format: "json",
		Writer: &buf,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	in := make(chan Message, 1)
	in <- Message{
		Table:  "users",
		Action: "INSERT",
		Columns: map[string]interface{}{"id": 1},
	}
	close(in)

	ctx := context.Background()
	if err := p.Run(ctx, in); err != nil {
		t.Fatalf("Run error: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "users") {
		t.Errorf("expected output to contain table name, got: %s", out)
	}
	if !strings.Contains(out, "INSERT") {
		t.Errorf("expected output to contain action, got: %s", out)
	}
}

func TestPipeline_FiltersMessage(t *testing.T) {
	var buf bytes.Buffer
	p, err := NewPipeline(PipelineConfig{
		Filter: FilterConfig{Tables: []string{"orders"}},
		Format: "json",
		Writer: &buf,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	in := make(chan Message, 1)
	in <- Message{Table: "users", Action: "INSERT", Columns: map[string]interface{}{}}
	close(in)

	if err := p.Run(context.Background(), in); err != nil {
		t.Fatalf("Run error: %v", err)
	}

	if buf.Len() != 0 {
		t.Errorf("expected no output for filtered message, got: %s", buf.String())
	}
}

func TestPipeline_UnknownFormat(t *testing.T) {
	_, err := NewPipeline(PipelineConfig{
		Format: "xml",
		Writer: &bytes.Buffer{},
	})
	if err == nil {
		t.Fatal("expected error for unknown format")
	}
}

func TestPipeline_ContextCancellation(t *testing.T) {
	var buf bytes.Buffer
	p, err := NewPipeline(PipelineConfig{
		Format: "text",
		Writer: &buf,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	in := make(chan Message)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = p.Run(ctx, in)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}
