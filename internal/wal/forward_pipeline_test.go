package wal

import (
	"strings"
	"testing"
)

func TestForwarder_IntegratesWithFilter(t *testing.T) {
	// Only INSERT on "payments" should reach the forwarder target.
	filter, _ := NewFilter(
		WithTables("payments"),
		WithActions("INSERT"),
	)

	var captured []*Message
	forwarder, _ := NewForwarder(
		func(m *Message) bool {
			return filter.Match(m)
		},
		WithForwardTarget(func(m *Message) error {
			captured = append(captured, m)
			return nil
		}),
	)

	msgs := []*Message{
		{Relation: "payments", Action: "INSERT"},
		{Relation: "payments", Action: "UPDATE"},
		{Relation: "users", Action: "INSERT"},
		{Relation: "payments", Action: "INSERT"},
	}

	for _, m := range msgs {
		_, _ = forwarder.Apply(m)
	}

	if len(captured) != 2 {
		t.Fatalf("expected 2 captured messages, got %d", len(captured))
	}
	for _, m := range captured {
		if m.Relation != "payments" || m.Action != "INSERT" {
			t.Errorf("unexpected message: %+v", m)
		}
	}
}

func TestForwarder_ErrorContainsContext(t *testing.T) {
	forwarder, _ := NewForwarder(
		func(*Message) bool { return true },
		WithForwardTarget(func(*Message) error {
			return errorf("downstream unavailable")
		}),
	)

	_, err := forwarder.Apply(&Message{Relation: "t", Action: "INSERT"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "forwarder") {
		t.Errorf("expected error to mention forwarder, got: %v", err)
	}
}
