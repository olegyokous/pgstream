package wal

import (
	"strings"
	"testing"
	"time"
)

func fixedTime() time.Time {
	return time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
}

func fixedID() string { return "test-id-001" }

func TestEnveloper_WrapSetsFields(t *testing.T) {
	env := NewEnveloper(
		WithEnvelopeSource("test-source"),
		withEnvelopeClock(fixedTime),
		withEnvelopeIDGen(fixedID),
	)

	e := env.Wrap(`{"action":"INSERT"}`, map[string]any{"table": "users"})

	if e.ID != "test-id-001" {
		t.Errorf("expected ID test-id-001, got %s", e.ID)
	}
	if e.Source != "test-source" {
		t.Errorf("expected source test-source, got %s", e.Source)
	}
	if !e.Timestamp.Equal(fixedTime()) {
		t.Errorf("unexpected timestamp: %v", e.Timestamp)
	}
	if e.Payload != `{"action":"INSERT"}` {
		t.Errorf("unexpected payload: %s", e.Payload)
	}
	if e.Meta["table"] != "users" {
		t.Errorf("expected meta table=users")
	}
}

func TestEnveloper_DefaultSource(t *testing.T) {
	env := NewEnveloper(withEnvelopeClock(fixedTime), withEnvelopeIDGen(fixedID))
	e := env.Wrap("hello", nil)
	if e.Source != "pgstream" {
		t.Errorf("expected default source pgstream, got %s", e.Source)
	}
}

func TestEnveloper_NilMetaIsAllowed(t *testing.T) {
	env := NewEnveloper(withEnvelopeClock(fixedTime), withEnvelopeIDGen(fixedID))
	e := env.Wrap("payload", nil)
	if e.Meta != nil {
		t.Errorf("expected nil meta")
	}
}

func TestEnvelope_StringContainsFields(t *testing.T) {
	env := NewEnveloper(
		WithEnvelopeSource("pgstream"),
		withEnvelopeClock(fixedTime),
		withEnvelopeIDGen(fixedID),
	)
	e := env.Wrap("my-payload", nil)
	s := e.String()

	for _, want := range []string{"test-id-001", "pgstream", "my-payload", "2024-06-01"} {
		if !strings.Contains(s, want) {
			t.Errorf("String() missing %q, got: %s", want, s)
		}
	}
}

func TestNewULID_IsUnique(t *testing.T) {
	seen := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		id := newULID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate ULID generated: %s", id)
		}
		seen[id] = struct{}{}
	}
}
