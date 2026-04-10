package wal

import (
	"testing"
)

func baseMsg() *Message {
	return &Message{
		Table:  "users",
		Action: "INSERT",
		Columns: []Column{
			{Name: "id", Value: "1"},
			{Name: "email", Value: "alice@example.com"},
			{Name: "password", Value: "secret"},
		},
	}
}

func TestTransformer_ApplyEmpty(t *testing.T) {
	tr := NewTransformer()
	msg := baseMsg()
	out := tr.Apply(msg)
	if out == nil {
		t.Fatal("expected non-nil message")
	}
}

func TestMaskColumns_MasksMatchingColumns(t *testing.T) {
	tr := NewTransformer(MaskColumns("password", "email"))
	out := tr.Apply(baseMsg())
	if out == nil {
		t.Fatal("unexpected nil")
	}
	for _, col := range out.Columns {
		if col.Name == "password" || col.Name == "email" {
			if col.Value != "***" {
				t.Errorf("expected *** for %s, got %s", col.Name, col.Value)
			}
		}
		if col.Name == "id" && col.Value != "1" {
			t.Errorf("id should not be masked, got %s", col.Value)
		}
	}
}

func TestMaskColumns_NilPassthrough(t *testing.T) {
	fn := MaskColumns("password")
	if fn(nil) != nil {
		t.Fatal("expected nil")
	}
}

func TestRenameTable_RenamesMatchingTable(t *testing.T) {
	tr := NewTransformer(RenameTable("users", "accounts"))
	out := tr.Apply(baseMsg())
	if out.Table != "accounts" {
		t.Errorf("expected accounts, got %s", out.Table)
	}
}

func TestRenameTable_CaseInsensitive(t *testing.T) {
	tr := NewTransformer(RenameTable("USERS", "accounts"))
	out := tr.Apply(baseMsg())
	if out.Table != "accounts" {
		t.Errorf("expected accounts, got %s", out.Table)
	}
}

func TestRenameTable_NoMatchLeavesSame(t *testing.T) {
	tr := NewTransformer(RenameTable("orders", "purchases"))
	out := tr.Apply(baseMsg())
	if out.Table != "users" {
		t.Errorf("expected users, got %s", out.Table)
	}
}

func TestDropAction_DropsMatchingAction(t *testing.T) {
	tr := NewTransformer(DropAction("INSERT"))
	out := tr.Apply(baseMsg())
	if out != nil {
		t.Fatal("expected nil message after drop")
	}
}

func TestDropAction_KeepsNonMatchingAction(t *testing.T) {
	tr := NewTransformer(DropAction("DELETE"))
	out := tr.Apply(baseMsg())
	if out == nil {
		t.Fatal("expected non-nil message")
	}
}

func TestTransformer_ChainStopsOnNil(t *testing.T) {
	called := false
	tr := NewTransformer(
		DropAction("INSERT"),
		func(msg *Message) *Message {
			called = true
			return msg
		},
	)
	out := tr.Apply(baseMsg())
	if out != nil {
		t.Fatal("expected nil")
	}
	if called {
		t.Fatal("second transform should not have been called")
	}
}
