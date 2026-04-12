package wal

import (
	"testing"
)

func labelMsg() *Message {
	return &Message{Table: "orders", Action: "INSERT"}
}

func TestNewLabeler_RequiresLabels(t *testing.T) {
	_, err := NewLabeler(nil)
	if err == nil {
		t.Fatal("expected error for empty labels")
	}
}

func TestLabeler_StampsMetaOnNilMap(t *testing.T) {
	l, err := NewLabeler(map[string]string{"env": "prod"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msg := labelMsg()
	out := l.Apply(msg)
	if out.Meta["env"] != "prod" {
		t.Errorf("expected meta env=prod, got %q", out.Meta["env"])
	}
}

func TestLabeler_NilMessagePassthrough(t *testing.T) {
	l, _ := NewLabeler(map[string]string{"k": "v"})
	if got := l.Apply(nil); got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
}

func TestLabeler_DoesNotOverwriteByDefault(t *testing.T) {
	l, _ := NewLabeler(map[string]string{"env": "prod"})
	msg := labelMsg()
	msg.Meta = map[string]string{"env": "staging"}
	l.Apply(msg)
	if msg.Meta["env"] != "staging" {
		t.Errorf("expected existing value to be preserved, got %q", msg.Meta["env"])
	}
}

func TestLabeler_OverwriteOption(t *testing.T) {
	l, _ := NewLabeler(map[string]string{"env": "prod"}, WithLabelOverwrite())
	msg := labelMsg()
	msg.Meta = map[string]string{"env": "staging"}
	l.Apply(msg)
	if msg.Meta["env"] != "prod" {
		t.Errorf("expected overwritten value prod, got %q", msg.Meta["env"])
	}
}

func TestLabeler_KeysNormalisedToLower(t *testing.T) {
	l, err := NewLabeler(map[string]string{"Region": "us-east-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msg := labelMsg()
	l.Apply(msg)
	if msg.Meta["region"] != "us-east-1" {
		t.Errorf("expected lower-case key 'region', meta=%v", msg.Meta)
	}
}

func TestLabeler_MultipleLabelsAllApplied(t *testing.T) {
	l, _ := NewLabeler(map[string]string{"env": "prod", "team": "platform"})
	msg := labelMsg()
	l.Apply(msg)
	if msg.Meta["env"] != "prod" || msg.Meta["team"] != "platform" {
		t.Errorf("not all labels applied: meta=%v", msg.Meta)
	}
}
