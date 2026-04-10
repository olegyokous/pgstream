package wal

import (
	"strings"
)

// TransformFunc is a function that transforms a Message in place.
type TransformFunc func(msg *Message) *Message

// Transformer applies a chain of TransformFuncs to each Message.
type Transformer struct {
	fns []TransformFunc
}

// NewTransformer creates a Transformer with the provided transform functions.
func NewTransformer(fns ...TransformFunc) *Transformer {
	return &Transformer{fns: fns}
}

// Apply runs all registered transform functions on msg in order.
// If any function returns nil the message is dropped (returns nil).
func (t *Transformer) Apply(msg *Message) *Message {
	for _, fn := range t.fns {
		msg = fn(msg)
		if msg == nil {
			return nil
		}
	}
	return msg
}

// MaskColumns returns a TransformFunc that replaces the value of each listed
// column with "***" to redact sensitive data.
func MaskColumns(columns ...string) TransformFunc {
	set := make(map[string]struct{}, len(columns))
	for _, c := range columns {
		set[c] = struct{}{}
	}
	return func(msg *Message) *Message {
		if msg == nil {
			return nil
		}
		for i, col := range msg.Columns {
			if _, ok := set[col.Name]; ok {
				msg.Columns[i].Value = "***"
			}
		}
		return msg
	}
}

// RenameTable returns a TransformFunc that rewrites Table to newName when it
// matches oldName (case-insensitive).
func RenameTable(oldName, newName string) TransformFunc {
	old := strings.ToLower(oldName)
	return func(msg *Message) *Message {
		if msg == nil {
			return nil
		}
		if strings.ToLower(msg.Table) == old {
			msg.Table = newName
		}
		return msg
	}
}

// DropAction returns a TransformFunc that drops messages whose Action matches
// any of the supplied actions (case-insensitive).
func DropAction(actions ...string) TransformFunc {
	set := make(map[string]struct{}, len(actions))
	for _, a := range actions {
		set[strings.ToLower(a)] = struct{}{}
	}
	return func(msg *Message) *Message {
		if msg == nil {
			return nil
		}
		if _, ok := set[strings.ToLower(msg.Action)]; ok {
			return nil
		}
		return msg
	}
}
