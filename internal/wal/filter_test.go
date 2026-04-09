package wal

import "testing"

func newMsg(schema, table string, action ActionType) *Message {
	return &Message{Schema: schema, Table: table, Action: action}
}

func TestFilter_NoConstraints(t *testing.T) {
	f := NewFilter(nil, nil)
	if !f.Match(newMsg("public", "users", ActionInsert)) {
		t.Error("expected match with no constraints")
	}
}

func TestFilter_TableMatch(t *testing.T) {
	f := NewFilter([]string{"public.users"}, nil)
	if !f.Match(newMsg("public", "users", ActionInsert)) {
		t.Error("expected match for public.users")
	}
	if f.Match(newMsg("public", "orders", ActionInsert)) {
		t.Error("expected no match for public.orders")
	}
}

func TestFilter_ActionMatch(t *testing.T) {
	f := NewFilter(nil, []ActionType{ActionInsert, ActionUpdate})
	if !f.Match(newMsg("public", "users", ActionInsert)) {
		t.Error("expected match for INSERT")
	}
	if f.Match(newMsg("public", "users", ActionDelete)) {
		t.Error("expected no match for DELETE")
	}
}

func TestFilter_TableAndActionMatch(t *testing.T) {
	f := NewFilter([]string{"public.users"}, []ActionType{ActionDelete})
	if f.Match(newMsg("public", "users", ActionInsert)) {
		t.Error("expected no match: wrong action")
	}
	if f.Match(newMsg("public", "orders", ActionDelete)) {
		t.Error("expected no match: wrong table")
	}
	if !f.Match(newMsg("public", "users", ActionDelete)) {
		t.Error("expected match for public.users DELETE")
	}
}

func TestFilter_NilMessage(t *testing.T) {
	f := NewFilter(nil, nil)
	if f.Match(nil) {
		t.Error("expected no match for nil message")
	}
}
