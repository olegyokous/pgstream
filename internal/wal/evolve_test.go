package wal

import (
	"errors"
	"testing"
)

func evolveMsg(table string) *Message {
	return &Message{
		Table:    table,
		Action:   "INSERT",
		Relation: &Relation{Table: table, Columns: []Column{{Name: "id", Type: "int4"}}},
	}
}

func TestNewEvolver_NilCallbackErrors(t *testing.T) {
	_, err := NewEvolver(nil)
	if err == nil {
		t.Fatal("expected error for nil callback")
	}
}

func TestEvolver_NilMessagePassthrough(t *testing.T) {
	e, _ := NewEvolver(func(string, int, *Message) error { return nil })
	out, err := e.Apply(nil)
	if err != nil || out != nil {
		t.Fatalf("expected nil, nil; got %v, %v", out, err)
	}
}

func TestEvolver_CallbackFiredOnFirstObserve(t *testing.T) {
	fired := 0
	e, _ := NewEvolver(func(_ string, _ int, _ *Message) error {
		fired++
		return nil
	})
	msg := evolveMsg("users")
	if _, err := e.Apply(msg); err != nil {
		t.Fatal(err)
	}
	if fired != 1 {
		t.Fatalf("expected callback fired 1 time, got %d", fired)
	}
}

func TestEvolver_CallbackNotFiredOnSameSchema(t *testing.T) {
	fired := 0
	e, _ := NewEvolver(func(_ string, _ int, _ *Message) error {
		fired++
		return nil
	})
	msg := evolveMsg("users")
	e.Apply(msg) //nolint
	e.Apply(msg) //nolint
	if fired != 1 {
		t.Fatalf("expected 1 callback, got %d", fired)
	}
}

func TestEvolver_CallbackFiredOnSchemaChange(t *testing.T) {
	fired := 0
	e, _ := NewEvolver(func(_ string, _ int, _ *Message) error {
		fired++
		return nil
	})
	msg1 := evolveMsg("users")
	msg2 := &Message{
		Table:    "users",
		Action:   "INSERT",
		Relation: &Relation{Table: "users", Columns: []Column{{Name: "id", Type: "int4"}, {Name: "email", Type: "text"}}},
	}
	e.Apply(msg1) //nolint
	e.Apply(msg2) //nolint
	if fired != 2 {
		t.Fatalf("expected 2 callbacks, got %d", fired)
	}
}

func TestEvolver_TableScopedSkipsOtherTables(t *testing.T) {
	fired := 0
	e, _ := NewEvolver(func(_ string, _ int, _ *Message) error {
		fired++
		return nil
	}, WithEvolverTable("users"))
	e.Apply(evolveMsg("orders")) //nolint
	if fired != 0 {
		t.Fatalf("expected 0 callbacks, got %d", fired)
	}
}

func TestEvolver_CallbackErrorPropagates(t *testing.T) {
	e, _ := NewEvolver(func(_ string, _ int, _ *Message) error {
		return errors.New("boom")
	})
	_, err := e.Apply(evolveMsg("users"))
	if err == nil {
		t.Fatal("expected error from callback")
	}
}
