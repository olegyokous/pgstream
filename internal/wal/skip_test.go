package wal

import (
	"testing"
)

func skipMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestNewSkipper_NoOptionsErrors(t *testing.T) {
	_, err := NewSkipper()
	if err == nil {
		t.Fatal("expected error when no options provided")
	}
}

func TestSkipper_NilMessagePassthrough(t *testing.T) {
	s, _ := NewSkipper(WithSkipTables("users"))
	if got := s.Apply(nil); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestSkipper_MatchingTableDropped(t *testing.T) {
	s, _ := NewSkipper(WithSkipTables("users"))
	msg := skipMsg("users", "INSERT")
	if got := s.Apply(msg); got != nil {
		t.Fatal("expected message to be dropped")
	}
}

func TestSkipper_NonMatchingTablePasses(t *testing.T) {
	s, _ := NewSkipper(WithSkipTables("users"))
	msg := skipMsg("orders", "INSERT")
	if got := s.Apply(msg); got == nil {
		t.Fatal("expected message to pass through")
	}
}

func TestSkipper_MatchingActionDropped(t *testing.T) {
	s, _ := NewSkipper(WithSkipActions("DELETE"))
	msg := skipMsg("orders", "DELETE")
	if got := s.Apply(msg); got != nil {
		t.Fatal("expected DELETE to be dropped")
	}
}

func TestSkipper_NonMatchingActionPasses(t *testing.T) {
	s, _ := NewSkipper(WithSkipActions("DELETE"))
	msg := skipMsg("orders", "INSERT")
	if got := s.Apply(msg); got == nil {
		t.Fatal("expected INSERT to pass through")
	}
}

func TestSkipper_CaseInsensitiveTable(t *testing.T) {
	s, _ := NewSkipper(WithSkipTables("Users"))
	msg := skipMsg("USERS", "UPDATE")
	if got := s.Apply(msg); got != nil {
		t.Fatal("expected case-insensitive match to drop message")
	}
}

func TestSkipper_TableAndActionBothChecked(t *testing.T) {
	s, _ := NewSkipper(WithSkipTables("audit"), WithSkipActions("TRUNCATE"))
	// action match should drop even if table does not match
	msg := skipMsg("orders", "TRUNCATE")
	if got := s.Apply(msg); got != nil {
		t.Fatal("expected TRUNCATE action to be dropped")
	}
	// table match should drop even if action does not match
	msg2 := skipMsg("audit", "INSERT")
	if got := s.Apply(msg2); got != nil {
		t.Fatal("expected audit table to be dropped")
	}
}
