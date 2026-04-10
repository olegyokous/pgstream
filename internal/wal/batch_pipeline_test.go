package wal

import (
	"strings"
	"testing"
	"time"
)

// TestBatcher_IntegratesWithMessages verifies that the Batcher correctly
// accumulates real Message values and delivers them to a downstream writer.
func TestBatcher_IntegratesWithMessages(t *testing.T) {
	inserts := []*Message{
		{Action: "INSERT", Table: "orders", Columns: map[string]any{"id": 1}},
		{Action: "INSERT", Table: "orders", Columns: map[string]any{"id": 2}},
		{Action: "UPDATE", Table: "orders", Columns: map[string]any{"id": 1, "status": "paid"}},
	}

	var received []*Message
	b := NewBatcher(
		BatchConfig{MaxSize: 10, MaxDelay: time.Minute},
		func(batch []*Message) error {
			received = append(received, batch...)
			return nil
		},
	)

	for _, m := range inserts {
		if err := b.Add(m); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if err := b.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if len(received) != len(inserts) {
		t.Fatalf("expected %d messages, got %d", len(inserts), len(received))
	}
	for i, m := range received {
		if m.Table != "orders" {
			t.Errorf("msg[%d]: unexpected table %q", i, m.Table)
		}
	}
}

// TestBatcher_DefaultConfigApplied ensures zero-value fields fall back to defaults.
func TestBatcher_DefaultConfigApplied(t *testing.T) {
	b := NewBatcher(BatchConfig{}, func(_ []*Message) error { return nil })
	def := DefaultBatchConfig()
	if b.cfg.MaxSize != def.MaxSize {
		t.Errorf("MaxSize: want %d, got %d", def.MaxSize, b.cfg.MaxSize)
	}
	if b.cfg.MaxDelay != def.MaxDelay {
		t.Errorf("MaxDelay: want %v, got %v", def.MaxDelay, b.cfg.MaxDelay)
	}
	_ = strings.Repeat("x", 0) // keep strings import if needed
}
