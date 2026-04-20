package wal

import (
	"bytes"
	"strings"
	"testing"
)

func TestTracer_IntegratesWithLabeler(t *testing.T) {
	var buf bytes.Buffer
	tracer := NewTracer(WithTracerWriter(&buf))
	labeler, err := NewLabeler(map[string]string{"env": "test"})
	if err != nil {
		t.Fatalf("NewLabeler: %v", err)
	}

	msg := &Message{Table: "events", Action: "INSERT", LSN: 10}
	out, err := tracer.Apply(msg)
	if err != nil {
		t.Fatalf("tracer.Apply: %v", err)
	}
	out, err = labeler.Apply(out)
	if err != nil {
		t.Fatalf("labeler.Apply: %v", err)
	}

	if out.Meta["trace_id"] == "" {
		t.Fatal("expected trace_id after tracer")
	}
	if out.Meta["env"] != "test" {
		t.Fatalf("expected env=test from labeler, got %s", out.Meta["env"])
	}
	if !strings.Contains(buf.String(), "table=events") {
		t.Fatal("expected trace output to mention table=events")
	}
}

func TestTracer_HopAccumulatesAcrossMultipleApplies(t *testing.T) {
	var buf bytes.Buffer
	tr := NewTracer(WithTracerWriter(&buf))

	msg := &Message{Table: "audit", Action: "UPDATE", LSN: 99}
	var err error
	for i := 1; i <= 4; i++ {
		msg, err = tr.Apply(msg)
		if err != nil {
			t.Fatalf("Apply hop %d: %v", i, err)
		}
		expected := strings.Repeat("x", 0) // just ensure no panic
		_ = expected
	}
	if msg.Meta["trace_hop"] != "4" {
		t.Fatalf("expected trace_hop=4 after 4 applies, got %s", msg.Meta["trace_hop"])
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 trace lines, got %d", len(lines))
	}
}
