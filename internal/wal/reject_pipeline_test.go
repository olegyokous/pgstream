package wal

import (
	"strings"
	"testing"
)

func TestRejecter_IntegratesWithFilter(t *testing.T) {
	rejecter, _ := NewRejecter([]RejectRule{
		{Table: "audit_log", Reason: "read-only table"},
	})

	msgs := []*Message{
		{Table: "users", Action: "INSERT"},
		{Table: "audit_log", Action: "INSERT"},
		{Table: "orders", Action: "UPDATE"},
	}

	var passed, rejected int
	for _, m := range msgs {
		if err := rejecter.Apply(m); err != nil {
			rejected++
		} else {
			passed++
		}
	}

	if passed != 2 {
		t.Errorf("expected 2 passed, got %d", passed)
	}
	if rejected != 1 {
		t.Errorf("expected 1 rejected, got %d", rejected)
	}
}

func TestRejecter_ErrorContainsReason(t *testing.T) {
	rejecter, _ := NewRejecter([]RejectRule{
		{Table: "secrets", Reason: "sensitive data"},
	})

	err := rejecter.Apply(&Message{Table: "secrets", Action: "SELECT"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "sensitive data") {
		t.Errorf("expected reason in error, got: %v", err)
	}
}
