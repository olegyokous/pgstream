package wal

import (
	"testing"
)

func annotateMsg(table, action string) *Message {
	return &Message{Table: table, Action: Action(action)}
}

func TestNewAnnotator_NoRulesErrors(t *testing.T) {
	_, err := NewAnnotator(nil)
	if err == nil {
		t.Fatal("expected error for empty rules")
	}
}

func TestNewAnnotator_EmptyKeyErrors(t *testing.T) {
	_, err := NewAnnotator([]AnnotateRule{{Table: "users", Key: ""}})
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestAnnotator_NilMessagePassthrough(t *testing.T) {
	a, _ := NewAnnotator([]AnnotateRule{{Key: "env", Value: "prod"}})
	if got := a.Apply(nil); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestAnnotator_AppliesRuleToAllTables(t *testing.T) {
	a, _ := NewAnnotator([]AnnotateRule{{Key: "env", Value: "prod"}})
	msg := annotateMsg("orders", "INSERT")
	out := a.Apply(msg)
	if out.Meta["env"] != "prod" {
		t.Fatalf("expected meta env=prod, got %v", out.Meta)
	}
}

func TestAnnotator_TableScopedSkipsOtherTable(t *testing.T) {
	a, _ := NewAnnotator([]AnnotateRule{{Table: "users", Key: "pii", Value: "true"}})
	msg := annotateMsg("orders", "INSERT")
	out := a.Apply(msg)
	if _, ok := out.Meta["pii"]; ok {
		t.Fatal("expected no annotation on non-matching table")
	}
}

func TestAnnotator_ActionScopedMatchesCorrectly(t *testing.T) {
	a, _ := NewAnnotator([]AnnotateRule{{Action: "DELETE", Key: "risky", Value: "yes"}})
	msg := annotateMsg("orders", "DELETE")
	out := a.Apply(msg)
	if out.Meta["risky"] != "yes" {
		t.Fatalf("expected meta risky=yes, got %v", out.Meta)
	}
}

func TestAnnotator_MultipleRulesAllApplied(t *testing.T) {
	rules := []AnnotateRule{
		{Key: "env", Value: "staging"},
		{Table: "users", Key: "pii", Value: "true"},
	}
	a, _ := NewAnnotator(rules)
	msg := annotateMsg("users", "INSERT")
	out := a.Apply(msg)
	if out.Meta["env"] != "staging" || out.Meta["pii"] != "true" {
		t.Fatalf("expected both annotations, got %v", out.Meta)
	}
}

func TestAnnotator_ExistingMetaPreserved(t *testing.T) {
	a, _ := NewAnnotator([]AnnotateRule{{Key: "env", Value: "prod"}})
	msg := annotateMsg("orders", "UPDATE")
	msg.Meta = map[string]string{"existing": "value"}
	out := a.Apply(msg)
	if out.Meta["existing"] != "value" {
		t.Fatal("existing meta key was lost")
	}
}
