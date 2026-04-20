package wal

import (
	"bytes"
	"strings"
	"testing"
)

func traceMsg() *Message {
	return &Message{Table: "orders", Action: "INSERT", LSN: 42}
}

func TestTracer_NilMessagePassthrough(t *testing.T) {
	tr := NewTracer()
	out, err := tr.Apply(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Fatal("expected nil output for nil input")
	}
}

func TestTracer_StampsTraceID(t *testing.T) {
	var buf bytes.Buffer
	tr := NewTracer(WithTracerWriter(&buf))
	msg := traceMsg()
	out, err := tr.Apply(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Meta["trace_id"] == "" {
		t.Fatal("expected trace_id to be set")
	}
	if out.Meta["trace_hop"] != "1" {
		t.Fatalf("expected trace_hop=1, got %s", out.Meta["trace_hop"])
	}
}

func TestTracer_PreservesExistingTraceID(t *testing.T) {
	var buf bytes.Buffer
	tr := NewTracer(WithTracerWriter(&buf))
	msg := traceMsg()
	msg.Meta = map[string]string{"trace_id": "fixed-id"}
	out, err := tr.Apply(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Meta["trace_id"] != "fixed-id" {
		t.Fatalf("expected trace_id to be preserved, got %s", out.Meta["trace_id"])
	}
}

func TestTracer_HopIncrements(t *testing.T) {
	var buf bytes.Buffer
	tr := NewTracer(WithTracerWriter(&buf))
	msg := traceMsg()
	msg.Meta = map[string]string{"trace_id": "x", "trace_hop": "3"}
	out, _ := tr.Apply(msg)
	if out.Meta["trace_hop"] != "4" {
		t.Fatalf("expected hop=4, got %s", out.Meta["trace_hop"])
	}
}

func TestTracer_WritesOutputLine(t *testing.T) {
	var buf bytes.Buffer
	tr := NewTracer(WithTracerWriter(&buf))
	_, _ = tr.Apply(traceMsg())
	line := buf.String()
	if !strings.Contains(line, "[trace]") {
		t.Fatalf("expected [trace] prefix in output, got: %s", line)
	}
	if !strings.Contains(line, "table=orders") {
		t.Fatalf("expected table=orders in output, got: %s", line)
	}
}

func TestTracer_TableScopedSkipsOtherTables(t *testing.T) {
	var buf bytes.Buffer
	tr := NewTracer(WithTracerWriter(&buf), WithTracerTable("payments"))
	msg := traceMsg() // table=orders
	out, _ := tr.Apply(msg)
	if out.Meta != nil && out.Meta["trace_id"] != "" {
		t.Fatal("expected no trace_id for non-matching table")
	}
	if buf.Len() != 0 {
		t.Fatal("expected no output for non-matching table")
	}
}

func TestTracer_CustomFieldName(t *testing.T) {
	var buf bytes.Buffer
	tr := NewTracer(WithTracerWriter(&buf), WithTracerField("x_trace"))
	msg := traceMsg()
	out, _ := tr.Apply(msg)
	if out.Meta["x_trace"] == "" {
		t.Fatal("expected x_trace to be set")
	}
}
