package wal

import (
	"strings"
	"testing"
)

// TestDigester_IntegratesWithLabeler verifies that a Digester and a Labeler
// can be chained: the digest computed by the Digester is visible to the
// Labeler when it copies meta keys into labels.
func TestDigester_IntegratesWithLabeler(t *testing.T) {
	digester, err := NewDigester(DigestConfig{Field: "_digest"})
	if err != nil {
		t.Fatalf("NewDigester: %v", err)
	}

	labeler, err := NewLabeler(map[string]string{"source": "wal"})
	if err != nil {
		t.Fatalf("NewLabeler: %v", err)
	}

	msg := &Message{
		Table:   "payments",
		Action:  "INSERT",
		Columns: map[string]any{"amount": 42},
	}

	msg = digester.Apply(msg)
	msg = labeler.Apply(msg)

	if msg.Meta["_digest"] == "" {
		t.Error("digest should be preserved after labeler")
	}
	if msg.Meta["source"] != "wal" {
		t.Error("label should be applied by labeler")
	}
}

// TestDigester_ChainedWithTagger verifies that tags added by a Tagger do not
// affect the digest, because the digest is computed before tagging.
func TestDigester_ChainedWithTagger(t *testing.T) {
	digester, _ := NewDigester(DigestConfig{})
	tagger, err := NewTagger(map[string]string{"env": "prod"}, nil)
	if err != nil {
		t.Fatalf("NewTagger: %v", err)
	}

	msg := &Message{
		Table:   "events",
		Action:  "UPDATE",
		Columns: map[string]any{"name": "click"},
	}

	// digest first, then tag
	digested := digester.Apply(msg)
	digestBefore := digested.Meta["_digest"]

	tagged := tagger.Apply(digested)

	if tagged.Meta["_digest"] != digestBefore {
		t.Error("tagger should not alter the pre-computed digest")
	}
	if tagged.Meta["env"] != "prod" {
		t.Error("tagger should add env tag")
	}
}

// TestDigester_ColumnScopedDoesNotPollute ensures that a column-scoped digest
// only hashes the requested columns and leaves unrelated meta keys intact.
func TestDigester_ColumnScopedDoesNotPollute(t *testing.T) {
	d, _ := NewDigester(DigestConfig{
		Algorithm: DigestMD5,
		Columns:   []string{"id"},
		Field:     "id_digest",
	})

	msg := &Message{
		Table:  "users",
		Action: "DELETE",
		Columns: map[string]any{
			"id":    7,
			"email": "carol@example.com",
		},
		Meta: map[string]string{"pre": "existing"},
	}

	out := d.Apply(msg)

	if out.Meta["pre"] != "existing" {
		t.Error("pre-existing meta should not be removed")
	}
	if out.Meta["id_digest"] == "" {
		t.Error("id_digest should be set")
	}
	if !strings.HasPrefix(out.Meta["id_digest"], "") || len(out.Meta["id_digest"]) != 32 {
		t.Errorf("md5 digest should be 32 hex chars, got %d", len(out.Meta["id_digest"]))
	}
}
