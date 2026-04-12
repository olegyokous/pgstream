package wal

import (
	"testing"
)

func enrichMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestNewEnricher_RequiresRules(t *testing.T) {
	_, err := NewEnricher(nil, nil)
	if err == nil {
		t.Fatal("expected error for empty rules")
	}
}

func TestEnricher_AppliesRuleToAllTables(t *testing.T) {
	e, err := NewEnricher([]EnrichRule{{Key: "env", Value: "prod"}}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	msg := enrichMsg("orders", "INSERT")
	out := e.Apply(msg)
	if out.Meta["env"] != "prod" {
		t.Errorf("expected meta env=prod, got %q", out.Meta["env"])
	}
}

func TestEnricher_SkipsNonMatchingTable(t *testing.T) {
	e, _ := NewEnricher([]EnrichRule{{Key: "region", Value: "us"}}, []string{"orders"})
	msg := enrichMsg("users", "INSERT")
	out := e.Apply(msg)
	if out.Meta != nil && out.Meta["region"] != "" {
		t.Errorf("expected no enrichment for non-matching table")
	}
}

func TestEnricher_MatchesTableCaseInsensitive(t *testing.T) {
	e, _ := NewEnricher([]EnrichRule{{Key: "src", Value: "pg"}}, []string{"Orders"})
	msg := enrichMsg("orders", "UPDATE")
	out := e.Apply(msg)
	if out.Meta["src"] != "pg" {
		t.Errorf("expected enrichment for case-insensitive table match")
	}
}

func TestEnricher_NilMessagePassthrough(t *testing.T) {
	e, _ := NewEnricher([]EnrichRule{{Key: "k", Value: "v"}}, nil)
	if got := e.Apply(nil); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestEnricher_MultipleRulesAllApplied(t *testing.T) {
	rules := []EnrichRule{
		{Key: "env", Value: "staging"},
		{Key: "team", Value: "platform"},
	}
	e, _ := NewEnricher(rules, nil)
	msg := enrichMsg("events", "DELETE")
	out := e.Apply(msg)
	if out.Meta["env"] != "staging" || out.Meta["team"] != "platform" {
		t.Errorf("expected both rules applied, got %v", out.Meta)
	}
}

func TestEnricher_PreservesExistingMeta(t *testing.T) {
	e, _ := NewEnricher([]EnrichRule{{Key: "new", Value: "val"}}, nil)
	msg := enrichMsg("tbl", "INSERT")
	msg.Meta = map[string]string{"existing": "yes"}
	out := e.Apply(msg)
	if out.Meta["existing"] != "yes" {
		t.Errorf("expected existing meta preserved")
	}
	if out.Meta["new"] != "val" {
		t.Errorf("expected new meta added")
	}
}
