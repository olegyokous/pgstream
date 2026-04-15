package wal_test

import (
	"testing"

	"github.com/your-org/pgstream/internal/wal"
)

func TestTruncator_IntegratesWithFormatter(t *testing.T) {
	msg := &wal.Message{
		Table:  "events",
		Action: "INSERT",
		Columns: []wal.Column{
			{Name: "id", Value: "1"},
			{Name: "payload", Value: "hello world"},
		},
	}

	truncator, err := wal.NewTruncator(wal.WithMaxBytes(1024), wal.WithTruncateAction(wal.TruncateStub))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out, err := truncator.Apply(msg)
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	if out == nil {
		t.Fatal("expected message, got nil")
	}
	if out.Table != "events" {
		t.Errorf("expected table 'events', got %q", out.Table)
	}
}

func TestTruncator_OversizedDroppedInChain(t *testing.T) {
	msg := &wal.Message{
		Table:  "logs",
		Action: "INSERT",
		Columns: []wal.Column{
			{Name: "data", Value: "x"},
		},
	}

	// Set max bytes to 1 so any realistic message is oversized.
	truncator, err := wal.NewTruncator(wal.WithMaxBytes(1), wal.WithTruncateAction(wal.TruncateDrop))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out, err := truncator.Apply(msg)
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	if out != nil {
		t.Errorf("expected nil (dropped), got message for table %q", out.Table)
	}
}

func TestTruncator_ChainedWithSkipper(t *testing.T) {
	skipper, err := wal.NewSkipper(wal.WithSkipTables("audit"))
	if err != nil {
		t.Fatalf("NewSkipper error: %v", err)
	}

	truncator, err := wal.NewTruncator(wal.WithMaxBytes(512), wal.WithTruncateAction(wal.TruncateDrop))
	if err != nil {
		t.Fatalf("NewTruncator error: %v", err)
	}

	msgs := []*wal.Message{
		{Table: "audit", Action: "INSERT", Columns: []wal.Column{{Name: "k", Value: "v"}}},
		{Table: "orders", Action: "UPDATE", Columns: []wal.Column{{Name: "k", Value: "v"}}},
	}

	var passed []*wal.Message
	for _, m := range msgs {
		out, err := skipper.Apply(m)
		if err != nil {
			t.Fatalf("Skipper.Apply error: %v", err)
		}
		if out == nil {
			continue
		}
		out2, err := truncator.Apply(out)
		if err != nil {
			t.Fatalf("Truncator.Apply error: %v", err)
		}
		if out2 != nil {
			passed = append(passed, out2)
		}
	}

	if len(passed) != 1 {
		t.Fatalf("expected 1 message through chain, got %d", len(passed))
	}
	if passed[0].Table != "orders" {
		t.Errorf("expected 'orders', got %q", passed[0].Table)
	}
}
