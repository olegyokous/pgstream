package wal

import (
	"testing"
)

// TestRouter_TablePartitioning verifies that a router can split traffic
// between multiple sinks based on table name, simulating a fan-out pattern.
func TestRouter_TablePartitioning(t *testing.T) {
	router := NewRouter()

	collected := map[string][]*Message{}
	for _, tbl := range []string{"orders", "users"} {
		table := tbl
		router.AddRoute(&Route{
			Name:      "sink-" + table,
			Predicate: func(m *Message) bool { return m.Table == table },
			Sink: func(m *Message) error {
				collected[table] = append(collected[table], m)
				return nil
			},
		})
	}

	messages := []*Message{
		{Table: "orders", Action: "INSERT"},
		{Table: "users", Action: "UPDATE"},
		{Table: "orders", Action: "DELETE"},
		{Table: "users", Action: "INSERT"},
		{Table: "products", Action: "INSERT"}, // no matching route
	}

	for _, m := range messages {
		if err := router.Dispatch(m); err != nil {
			t.Fatalf("dispatch error: %v", err)
		}
	}

	if len(collected["orders"]) != 2 {
		t.Errorf("orders: expected 2 messages, got %d", len(collected["orders"]))
	}
	if len(collected["users"]) != 2 {
		t.Errorf("users: expected 2 messages, got %d", len(collected["users"]))
	}
	if _, ok := collected["products"]; ok {
		t.Error("products should not have been routed")
	}
}
