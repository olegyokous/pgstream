package wal

import (
	"errors"
	"testing"
)

func TestRouter_DispatchesToAllMatchingRoutes(t *testing.T) {
	router := NewRouter()
	hits := map[string]int{}

	for _, name := range []string{"a", "b"} {
		n := name
		router.AddRoute(&Route{
			Name:      n,
			Predicate: func(*Message) bool { return true },
			Sink:      func(*Message) error { hits[n]++; return nil },
		})
	}

	msg := &Message{Table: "users", Action: "INSERT"}
	if err := router.Dispatch(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, name := range []string{"a", "b"} {
		if hits[name] != 1 {
			t.Errorf("route %q: expected 1 hit, got %d", name, hits[name])
		}
	}
}

func TestRouter_SkipsNonMatchingRoute(t *testing.T) {
	router := NewRouter()
	hit := false
	router.AddRoute(&Route{
		Name:      "only-deletes",
		Predicate: func(m *Message) bool { return m.Action == "DELETE" },
		Sink:      func(*Message) error { hit = true; return nil },
	})

	if err := router.Dispatch(&Message{Action: "INSERT"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hit {
		t.Error("route should not have been called")
	}
}

func TestRouter_ReturnsFirstErrorButContinues(t *testing.T) {
	router := NewRouter()
	secondCalled := false

	router.AddRoute(&Route{
		Name: "fail",
		Sink: func(*Message) error { return errors.New("sink error") },
	})
	router.AddRoute(&Route{
		Name: "ok",
		Sink: func(*Message) error { secondCalled = true; return nil },
	})

	err := router.Dispatch(&Message{Action: "UPDATE"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !secondCalled {
		t.Error("second route should still be called after first error")
	}
}

func TestRouter_NilPredicateMatchesAll(t *testing.T) {
	router := NewRouter()
	hit := false
	router.AddRoute(&Route{
		Name: "catch-all",
		Sink: func(*Message) error { hit = true; return nil },
	})
	if err := router.Dispatch(&Message{Action: "INSERT"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hit {
		t.Error("catch-all route should have been called")
	}
}

func TestRouter_Len(t *testing.T) {
	router := NewRouter()
	if router.Len() != 0 {
		t.Fatalf("expected 0 routes, got %d", router.Len())
	}
	router.AddRoute(&Route{Name: "r1", Sink: func(*Message) error { return nil }})
	router.AddRoute(&Route{Name: "r2", Sink: func(*Message) error { return nil }})
	if router.Len() != 2 {
		t.Fatalf("expected 2 routes, got %d", router.Len())
	}
}
