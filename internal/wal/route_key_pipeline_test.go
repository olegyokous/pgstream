package wal

import "testing"

// TestRouteKey_IntegratesWithRouter verifies that RouteKey can drive
// a Router to dispatch messages to the correct handler based on the
// extracted key.
func TestRouteKey_IntegratesWithRouter(t *testing.T) {
	var insertSeen, updateSeen bool

	rk := NewRouteKey(WithRouteKeyStrategy("action"))

	router := NewRouter()
	router.Register(
		func(m *Message) bool { return rk.Extract(m) == "INSERT" },
		func(m *Message) error { insertSeen = true; return nil },
	)
	router.Register(
		func(m *Message) bool { return rk.Extract(m) == "UPDATE" },
		func(m *Message) error { updateSeen = true; return nil },
	)

	insertMsg := routeMsg("orders", "INSERT")
	if err := router.Dispatch(insertMsg); err != nil {
		t.Fatalf("unexpected error dispatching INSERT: %v", err)
	}

	updateMsg := routeMsg("orders", "UPDATE")
	if err := router.Dispatch(updateMsg); err != nil {
		t.Fatalf("unexpected error dispatching UPDATE: %v", err)
	}

	if !insertSeen {
		t.Error("expected INSERT handler to be called")
	}
	if !updateSeen {
		t.Error("expected UPDATE handler to be called")
	}
}

// TestRouteKey_TableActionKeyIsUnique verifies that different table/action
// combinations produce distinct keys.
func TestRouteKey_TableActionKeyIsUnique(t *testing.T) {
	rk := NewRouteKey(WithRouteKeyStrategy("table_action"))

	cases := []struct {
		table, action, want string
	}{
		{"users", "INSERT", "users.INSERT"},
		{"users", "DELETE", "users.DELETE"},
		{"orders", "INSERT", "orders.INSERT"},
	}

	for _, tc := range cases {
		got := rk.Extract(routeMsg(tc.table, tc.action))
		if got != tc.want {
			t.Errorf("Extract(%q, %q) = %q, want %q", tc.table, tc.action, got, tc.want)
		}
	}
}
