package wal

import (
	"testing"
)

func rejectMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestNewRejecter_NoRulesErrors(t *testing.T) {
	_, err := NewRejecter(nil)
	if err == nil {
		t.Fatal("expected error for empty rules")
	}
}

func TestNewRejecter_MissingReasonErrors(t *testing.T) {
	_, err := NewRejecter([]RejectRule{{Table: "users"}})
	if err == nil {
		t.Fatal("expected error for missing reason")
	}
}

func TestRejecter_NilMessagePassthrough(t *testing.T) {
	r, _ := NewRejecter([]RejectRule{{Reason: "always"}})
	if err := r.Apply(nil); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestRejecter_MatchingTableRejected(t *testing.T) {
	r, _ := NewRejecter([]RejectRule{{Table: "orders", Reason: "blocked table"}})
	err := r.Apply(rejectMsg("orders", "INSERT"))
	if err == nil {
		t.Fatal("expected rejection error")
	}
}

func TestRejecter_NonMatchingTablePasses(t *testing.T) {
	r, _ := NewRejecter([]RejectRule{{Table: "orders", Reason: "blocked table"}})
	err := r.Apply(rejectMsg("users", "INSERT"))
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestRejecter_ActionScopedReject(t *testing.T) {
	r, _ := NewRejecter([]RejectRule{{Action: "DELETE", Reason: "no deletes"}})
	if err := r.Apply(rejectMsg("any", "DELETE")); err == nil {
		t.Fatal("expected rejection")
	}
	if err := r.Apply(rejectMsg("any", "INSERT")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRejecter_TableAndActionBothMustMatch(t *testing.T) {
	r, _ := NewRejecter([]RejectRule{{Table: "orders", Action: "DELETE", Reason: "no order deletes"}})
	if err := r.Apply(rejectMsg("orders", "INSERT")); err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}
	if err := r.Apply(rejectMsg("orders", "DELETE")); err == nil {
		t.Fatal("expected rejection")
	}
}
