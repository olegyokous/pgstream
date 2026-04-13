package wal

import "strings"

// Flattener collapses a WAL message's columns into a single string map,
// optionally prefixing each key with the table name.
type Flattener struct {
	prefix    bool
	separator string
}

// FlattenOption configures a Flattener.
type FlattenOption func(*Flattener)

// WithFlattenPrefix enables table-name prefixing for each column key.
func WithFlattenPrefix() FlattenOption {
	return func(f *Flattener) { f.prefix = true }
}

// WithFlattenSeparator sets the separator used between table name and column
// when prefix mode is enabled. Defaults to ".".
func WithFlattenSeparator(sep string) FlattenOption {
	return func(f *Flattener) { f.separator = sep }
}

// NewFlattener returns a Flattener with the given options applied.
func NewFlattener(opts ...FlattenOption) *Flattener {
	f := &Flattener{separator: "."}
	for _, o := range opts {
		o(f)
	}
	return f
}

// Flatten converts the columns of msg into a flat map[string]string.
// Nil messages return a nil map. Nil column values are represented as empty
// strings.
func (f *Flattener) Flatten(msg *Message) map[string]string {
	if msg == nil {
		return nil
	}
	out := make(map[string]string, len(msg.Columns))
	for _, col := range msg.Columns {
		key := col.Name
		if f.prefix && msg.Table != "" {
			key = strings.Join([]string{msg.Table, col.Name}, f.separator)
		}
		val := ""
		if col.Value != nil {
			val = col.Value.(string)
		}
		out[key] = val
	}
	return out
}
