package wal

import "testing"

func routeMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestNewRouteKey_DefaultsToTable(t *testing.T) {
	rk := NewRouteKey()
	if rk.Strategy() != "table" {
		t.Fatalf("expected strategy 'table', got %q", rk.Strategy())
	}
}

func TestRouteKey_NilMessageReturnsEmpty(t *testing.T) {
	rk := NewRouteKey()
	if got := rk.Extract(nil); got != "" {
		t.Fatalf("expected empty string for nil message, got %q", got)
	}
}

func TestRouteKey_TableStrategy(t *testing.T) {
	rk := NewRouteKey(WithRouteKeyStrategy("table"))
	msg := routeMsg("orders", "INSERT")
	if got := rk.Extract(msg); got != "orders" {
		t.Fatalf("expected 'orders', got %q", got)
	}
}

func TestRouteKey_ActionStrategy(t *testing.T) {
	rk := NewRouteKey(WithRouteKeyStrategy("action"))
	msg := routeMsg("orders", "UPDATE")
	if got := rk.Extract(msg); got != "UPDATE" {
		t.Fatalf("expected 'UPDATE', got %q", got)
	}
}

func TestRouteKey_TableActionStrategy(t *testing.T) {
	rk := NewRouteKey(WithRouteKeyStrategy("table_action"))
	msg := routeMsg("users", "DELETE")
	want := "users.DELETE"
	if got := rk.Extract(msg); got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestRouteKey_UnknownStrategyFallsBackToTable(t *testing.T) {
	rk := NewRouteKey(WithRouteKeyStrategy("unknown"))
	msg := routeMsg("products", "INSERT")
	if got := rk.Extract(msg); got != "products" {
		t.Fatalf("expected 'products', got %q", got)
	}
}

func TestRouteKey_StrategyIsCaseInsensitive(t *testing.T) {
	rk := NewRouteKey(WithRouteKeyStrategy("ACTION"))
	if rk.Strategy() != "action" {
		t.Fatalf("expected normalised strategy 'action', got %q", rk.Strategy())
	}
}
