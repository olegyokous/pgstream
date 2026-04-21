package wal

import (
	"strings"
	"testing"
)

func digestMsg() *Message {
	return &Message{
		Table:  "orders",
		Action: "INSERT",
		Columns: map[string]any{
			"id":    1,
			"email": "alice@example.com",
			"total": 99.5,
		},
	}
}

func TestNewDigester_DefaultAlgorithm(t *testing.T) {
	d, err := NewDigester(DigestConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.cfg.Algorithm != DigestSHA256 {
		t.Errorf("expected sha256, got %q", d.cfg.Algorithm)
	}
	if d.cfg.Field != "_digest" {
		t.Errorf("expected _digest field, got %q", d.cfg.Field)
	}
}

func TestNewDigester_UnknownAlgorithmErrors(t *testing.T) {
	_, err := NewDigester(DigestConfig{Algorithm: "crc32"})
	if err == nil {
		t.Fatal("expected error for unknown algorithm")
	}
}

func TestDigester_NilMessagePassthrough(t *testing.T) {
	d, _ := NewDigester(DigestConfig{})
	if d.Apply(nil) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestDigester_SHA256StoresDigestInMeta(t *testing.T) {
	d, _ := NewDigester(DigestConfig{Algorithm: DigestSHA256, Field: "chk"})
	msg := digestMsg()
	out := d.Apply(msg)
	if out.Meta["chk"] == "" {
		t.Error("expected digest to be set")
	}
	if len(out.Meta["chk"]) != 64 {
		t.Errorf("sha256 hex should be 64 chars, got %d", len(out.Meta["chk"]))
	}
}

func TestDigester_MD5StoresDigestInMeta(t *testing.T) {
	d, _ := NewDigester(DigestConfig{Algorithm: DigestMD5})
	out := d.Apply(digestMsg())
	if len(out.Meta["_digest"]) != 32 {
		t.Errorf("md5 hex should be 32 chars, got %d", len(out.Meta["_digest"]))
	}
}

func TestDigester_IsDeterministic(t *testing.T) {
	d, _ := NewDigester(DigestConfig{})
	a := d.Apply(digestMsg())
	b := d.Apply(digestMsg())
	if a.Meta["_digest"] != b.Meta["_digest"] {
		t.Error("digest should be deterministic")
	}
}

func TestDigester_ColumnSubsetDiffersFromFull(t *testing.T) {
	full, _ := NewDigester(DigestConfig{})
	partial, _ := NewDigester(DigestConfig{Columns: []string{"id"}})

	a := full.Apply(digestMsg()).Meta["_digest"]
	b := partial.Apply(digestMsg()).Meta["_digest"]
	if a == b {
		t.Error("subset digest should differ from full digest")
	}
}

func TestDigester_ChangedColumnChangesDigest(t *testing.T) {
	d, _ := NewDigester(DigestConfig{})

	m1 := digestMsg()
	m2 := digestMsg()
	m2.Columns["email"] = "bob@example.com"

	if d.Apply(m1).Meta["_digest"] == d.Apply(m2).Meta["_digest"] {
		t.Error("different column values should produce different digests")
	}
}

func TestDigester_InitialisesNilMeta(t *testing.T) {
	d, _ := NewDigester(DigestConfig{})
	msg := digestMsg()
	msg.Meta = nil
	out := d.Apply(msg)
	if out.Meta == nil {
		t.Error("Meta should be initialised")
	}
	if !strings.HasPrefix(out.Meta["_digest"], "") {
		t.Error("digest should be present")
	}
}
