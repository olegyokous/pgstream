package wal

import (
	"testing"
)

func cloneBase() *Message {
	return &Message{
		LSN:    42,
		Table:  "orders",
		Action: "INSERT",
		Columns: map[string]any{"id": 1, "status": "new"},
		Meta:    map[string]string{"env": "prod"},
	}
}

func TestCloner_NilMessageReturnsNil(t *testing.T) {
	c := NewCloner()
	out, err := c.Clone(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Fatalf("expected nil, got %+v", out)
	}
}

func TestCloner_ProducesDeepCopy(t *testing.T) {
	c := NewCloner()
	src := cloneBase()
	out, err := c.Clone(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == src {
		t.Fatal("expected a new pointer, got the same")
	}
	if out.Table != src.Table || out.Action != src.Action || out.LSN != src.LSN {
		t.Fatalf("scalar fields differ: %+v vs %+v", out, src)
	}
	// Mutating the copy must not affect the original.
	out.Columns["id"] = 999
	if src.Columns["id"] == 999 {
		t.Fatal("mutation of clone affected source columns")
	}
	out.Meta["env"] = "staging"
	if src.Meta["env"] == "staging" {
		t.Fatal("mutation of clone affected source meta")
	}
}

func TestCloner_TableConstraintSkipsNonMatch(t *testing.T) {
	c := NewCloner(WithCloneTable("users"))
	src := cloneBase() // table == "orders"
	out, err := c.Clone(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != src {
		t.Fatal("expected original pointer when table does not match")
	}
}

func TestCloner_TableConstraintClonesMatch(t *testing.T) {
	c := NewCloner(WithCloneTable("orders"))
	src := cloneBase()
	out, err := c.Clone(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == src {
		t.Fatal("expected a new pointer for matching table")
	}
}

func TestCloner_ActionConstraintSkipsNonMatch(t *testing.T) {
	c := NewCloner(WithCloneAction("DELETE"))
	src := cloneBase() // action == "INSERT"
	out, err := c.Clone(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != src {
		t.Fatal("expected original pointer when action does not match")
	}
}

func TestCloner_NilColumnsAndMetaAreSafe(t *testing.T) {
	c := NewCloner()
	src := &Message{LSN: 1, Table: "t", Action: "INSERT"}
	out, err := c.Clone(src)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Columns != nil {
		t.Fatal("expected nil columns in clone")
	}
	if out.Meta != nil {
		t.Fatal("expected nil meta in clone")
	}
}
