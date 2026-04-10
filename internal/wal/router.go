package wal

import "strings"

// Route defines a destination for WAL messages matching a given predicate.
type Route struct {
	Name      string
	Predicate func(*Message) bool
	Sink      func(*Message) error
}

// Router fans out WAL messages to one or more named routes based on predicates.
// A message is delivered to every route whose predicate returns true.
type Router struct {
	routes []*Route
}

// NewRouter returns an empty Router.
func NewRouter() *Router {
	return &Router{}
}

// AddRoute registers a new route. Panics if name is empty or sink is nil.
func (r *Router) AddRoute(route *Route) {
	if strings.TrimSpace(route.Name) == "" {
		panic("router: route name must not be empty")
	}
	if route.Sink == nil {
		panic("router: route sink must not be nil")
	}
	if route.Predicate == nil {
		route.Predicate = func(*Message) bool { return true }
	}
	r.routes = append(r.routes, route)
}

// Dispatch sends msg to every matching route and returns the first error
// encountered, continuing delivery to remaining routes regardless.
func (r *Router) Dispatch(msg *Message) error {
	var firstErr error
	for _, route := range r.routes {
		if route.Predicate(msg) {
			if err := route.Sink(msg); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// Len returns the number of registered routes.
func (r *Router) Len() int { return len(r.routes) }
