package wal

import "strings"

// Skipper drops messages whose table or action matches a configured deny-list.
// It is the inverse of a filter allowlist: anything matched is discarded.
type Skipper struct {
	tables  []string
	actions []string
}

// SkipperOption configures a Skipper.
type SkipperOption func(*Skipper)

// WithSkipTables adds table names (case-insensitive) that should be dropped.
func WithSkipTables(tables ...string) SkipperOption {
	return func(s *Skipper) {
		for _, t := range tables {
			s.tables = append(s.tables, strings.ToLower(t))
		}
	}
}

// WithSkipActions adds action names (case-insensitive) that should be dropped.
func WithSkipActions(actions ...string) SkipperOption {
	return func(s *Skipper) {
		for _, a := range actions {
			s.actions = append(s.actions, strings.ToLower(a))
		}
	}
}

// NewSkipper constructs a Skipper with the given options.
// At least one option must configure a non-empty deny-list.
func NewSkipper(opts ...SkipperOption) (*Skipper, error) {
	s := &Skipper{}
	for _, o := range opts {
		o(s)
	}
	if len(s.tables) == 0 && len(s.actions) == 0 {
		return nil, errorf("skipper: at least one table or action must be specified")
	}
	return s, nil
}

// Apply returns nil (drop) when the message matches any deny-list entry,
// otherwise it returns the message unchanged.
func (s *Skipper) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	table := strings.ToLower(msg.Table)
	for _, t := range s.tables {
		if t == table {
			return nil
		}
	}
	action := strings.ToLower(msg.Action)
	for _, a := range s.actions {
		if a == action {
			return nil
		}
	}
	return msg
}
