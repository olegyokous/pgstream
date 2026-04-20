package wal

import (
	"testing"
)

// TestCorrelater_IntegratesWithLabeler verifies that a Correlater can stamp a
// correlation ID that is subsequently readable by a Labeler operating on Meta.
func TestCorrelater_IntegratesWithLabeler(t *testing.T) {
	corr, err := NewCorrelater("request_id", "cid")
	if err != nil {
		t.Fatalf("NewCorrelater: %v", err)
	}

	msg := &Message{
		Table:  "events",
		Action: "INSERT",
		Columns: []Column{
			{Name: "request_id", Value: "req-42"},
		},
	}

	out, err := corr.Apply(msg)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Meta["cid"] != "req-42" {
		t.Fatalf("cid not stamped: got %v", out.Meta["cid"])
	}

	// downstream consumer reads the correlation id from meta
	got, ok := out.Meta["cid"]
	if !ok {
		t.Fatal("cid key missing from meta")
	}
	if got != "req-42" {
		t.Fatalf("expected req-42 got %v", got)
	}
}

// TestCorrelater_ChainedWithTagger verifies that a tagger applied after a
// correlater can see the meta key set by the correlater.
func TestCorrelater_ChainedWithTagger(t *testing.T) {
	corr, _ := NewCorrelater("trace_id", "trace")
	tagger, err := NewTagger("source", "wal")
	if err != nil {
		t.Fatalf("NewTagger: %v", err)
	}

	msg := &Message{
		Table:  "spans",
		Action: "INSERT",
		Columns: []Column{
			{Name: "trace_id", Value: "t-99"},
		},
	}

	after, err := corr.Apply(msg)
	if err != nil {
		t.Fatalf("corr.Apply: %v", err)
	}
	final, err := tagger.Apply(after)
	if err != nil {
		t.Fatalf("tagger.Apply: %v", err)
	}
	if final.Meta["trace"] != "t-99" {
		t.Fatalf("trace not set: %v", final.Meta["trace"])
	}
	if final.Meta["source"] != "wal" {
		t.Fatalf("source not set: %v", final.Meta["source"])
	}
}
