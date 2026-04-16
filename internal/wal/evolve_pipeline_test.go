package wal

import (
	"strings"
	"testing"
)

func TestEvolver_IntegratesWithSchemaVersion(t *testing.T) {
	var log []string
	e, err := NewEvolver(func(table string, version int, _ *Message) error {
		log = append(log, table)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	msgs := []*Message{
		evolveMsg("users"),
		evolveMsg("users"),
		evolveMsg("orders"),
	}
	for _, m := range msgs {
		if _, err := e.Apply(m); err != nil {
			t.Fatal(err)
		}
	}
	if len(log) != 2 {
		t.Fatalf("expected 2 callbacks (users + orders), got %d", len(log))
	}
}

func TestEvolver_MessagePassesThroughUnchanged(t *testing.T) {
	e, _ := NewEvolver(func(_ string, _ int, _ *Message) error { return nil })
	msg := evolveMsg("users")
	msg.Columns = []Column{{Name: "id", Value: "42"}}
	out, err := e.Apply(msg)
	if err != nil {
		t.Fatal(err)
	}
	if out == nil || len(out.Columns) != 1 || out.Columns[0].Value != "42" {
		t.Fatal("message was mutated or lost")
	}
}

func TestEvolver_ErrorContainsContext(t *testing.T) {
	e, _ := NewEvolver(func(_ string, _ int, _ *Message) error {
		return errorf("schema mismatch")
	})
	_, err := e.Apply(evolveMsg("users"))
	if err == nil || !strings.Contains(err.Error(), "evolver") {
		t.Fatalf("expected wrapped evolver error, got %v", err)
	}
}
