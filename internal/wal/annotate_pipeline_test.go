package wal

import (
	"strings"
	"testing"
)

func TestAnnotator_IntegratesWithLabeler(t *testing.T) {
	annotator, err := NewAnnotator([]AnnotateRule{
		{Key: "source", Value: "wal"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	labeler, err := NewLabeler(map[string]string{"region": "eu-west-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msg := annotateMsg("events", "INSERT")
	msg = annotator.Apply(msg)
	msg = labeler.Apply(msg)

	if msg.Meta["source"] != "wal" {
		t.Errorf("expected source=wal, got %v", msg.Meta["source"])
	}
	if msg.Meta["region"] != "eu-west-1" {
		t.Errorf("expected region=eu-west-1, got %v", msg.Meta["region"])
	}
}

func TestAnnotator_ChainedWithTagger(t *testing.T) {
	annotator, err := NewAnnotator([]AnnotateRule{
		{Table: "payments", Key: "compliance", Value: "pci"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tagger, err := NewTagger("pipeline", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msg := annotateMsg("payments", "INSERT")
	msg = annotator.Apply(msg)
	msg = tagger.Apply(msg)

	if msg.Meta["compliance"] != "pci" {
		t.Errorf("expected compliance=pci, got %v", msg.Meta)
	}
	if !strings.Contains(msg.Tags, "pipeline") {
		t.Errorf("expected tag 'pipeline' in %q", msg.Tags)
	}
}
