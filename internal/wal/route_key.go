package wal

import "strings"

// RouteKey extracts a routing key from a message based on a configured strategy.
// Supported strategies: "table", "action", "table_action".
type RouteKey struct {
	strategy string
}

// RouteKeyOption configures a RouteKey extractor.
type RouteKeyOption func(*RouteKey)

// WithRouteKeyStrategy sets the key extraction strategy.
func WithRouteKeyStrategy(s string) RouteKeyOption {
	return func(r *RouteKey) {
		r.strategy = strings.ToLower(s)
	}
}

// NewRouteKey creates a RouteKey extractor with the given options.
// Defaults to "table" strategy.
func NewRouteKey(opts ...RouteKeyOption) *RouteKey {
	rk := &RouteKey{strategy: "table"}
	for _, o := range opts {
		o(rk)
	}
	return rk
}

// Extract returns the routing key for the given message.
// Returns an empty string if msg is nil.
func (rk *RouteKey) Extract(msg *Message) string {
	if msg == nil {
		return ""
	}
	switch rk.strategy {
	case "action":
		return msg.Action
	case "table_action":
		return msg.Table + "." + msg.Action
	default: // "table"
		return msg.Table
	}
}

// Strategy returns the currently configured strategy.
func (rk *RouteKey) Strategy() string {
	return rk.strategy
}
