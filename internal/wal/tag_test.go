package wal

import (
	"testing"
)

func tagMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestTagger_StaticTagAppliedToAll(t *testing.T) {
	tagger := NewTagger(Tag{Key: "env", Value: "prod"})
	msg := tagMsg("orders", "INSERT")
	out := tagger.Apply(msg)
	if out.Meta["env"] != "prod" {
		t.Fatalf("expected env=prod, got %q", out.Meta["env"])
	}
}

func TestTagger_NilMessagePassthrough(t *testing.T) {
	tagger := NewTagger(Tag{Key: "env", Value: "test"})
	if got := tagger.Apply(nil); got != nil {
		t.Fatal("expected nil")
	}
}

func TestTagger_RuleMatchesTableAndAction(t *testing.T) {
	tagger := NewTagger()
	tagger.AddRule("orders", "INSERT", Tag{Key: "priority", Value: "high"})
	msg := tagMsg("orders", "INSERT")
	out := tagger.Apply(msg)
	if out.Meta["priority"] != "high" {
		t.Fatalf("expected priority=high, got %q", out.Meta["priority"])
	}
}

func TestTagger_RuleDoesNotMatchDifferentTable(t *testing.T) {
	tagger := NewTagger()
	tagger.AddRule("orders", "INSERT", Tag{Key: "priority", Value: "high"})
	msg := tagMsg("users", "INSERT")
	out := tagger.Apply(msg)
	if _, ok := out.Meta["priority"]; ok {
		t.Fatal("expected no priority tag")
	}
}

func TestTagger_WildcardTableMatchesAll(t *testing.T) {
	tagger := NewTagger()
	tagger.AddRule("", "DELETE", Tag{Key: "op", Value: "delete"})
	for _, tbl := range []string{"orders", "users", "products"} {
		msg := tagMsg(tbl, "DELETE")
		out := tagger.Apply(msg)
		if out.Meta["op"] != "delete" {
			t.Fatalf("table %s: expected op=delete", tbl)
		}
	}
}

func TestTagger_CaseInsensitiveMatch(t *testing.T) {
	tagger := NewTagger()
	tagger.AddRule("Orders", "insert", Tag{Key: "x", Value: "1"})
	msg := tagMsg("ORDERS", "INSERT")
	out := tagger.Apply(msg)
	if out.Meta["x"] != "1" {
		t.Fatal("expected case-insensitive match")
	}
}

func TestTagger_MultipleRulesApplied(t *testing.T) {
	tagger := NewTagger(Tag{Key: "env", Value: "staging"})
	tagger.AddRule("orders", "", Tag{Key: "domain", Value: "commerce"})
	tagger.AddRule("", "DELETE", Tag{Key: "risky", Value: "true"})
	msg := tagMsg("orders", "DELETE")
	out := tagger.Apply(msg)
	if out.Meta["env"] != "staging" {
		t.Error("missing static tag")
	}
	if out.Meta["domain"] != "commerce" {
		t.Error("missing domain tag")
	}
	if out.Meta["risky"] != "true" {
		t.Error("missing risky tag")
	}
}
