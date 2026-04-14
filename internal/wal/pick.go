package wal

import "fmt"

// Picker selects a single named column value from a message and promotes it
// into the message metadata under a configurable key. This is useful for
// surfacing a natural identifier (e.g. "user_id") into downstream routing or
// labelling stages without requiring a full transformer chain.
type Picker struct {
	column  string
	metaKey string
	table   string // optional: only apply to this table
}

// PickerOption configures a Picker.
type PickerOption func(*Picker)

// WithPickerTable restricts the picker to messages from the given table.
func WithPickerTable(table string) PickerOption {
	return func(p *Picker) { p.table = table }
}

// WithPickerMetaKey overrides the metadata key used when storing the picked
// value. Defaults to the column name.
func WithPickerMetaKey(key string) PickerOption {
	return func(p *Picker) { p.metaKey = key }
}

// NewPicker constructs a Picker that extracts column from every matching
// message and stores the value in msg.Meta under metaKey.
func NewPicker(column string, opts ...PickerOption) (*Picker, error) {
	if column == "" {
		return nil, fmt.Errorf("picker: column must not be empty")
	}
	p := &Picker{column: column, metaKey: column}
	for _, o := range opts {
		o(p)
	}
	return p, nil
}

// Apply extracts the configured column value from msg and stores it in
// msg.Meta. If the column is absent the message is returned unchanged.
// A nil message is returned as-is.
func (p *Picker) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	if p.table != "" && msg.Table != p.table {
		return msg
	}
	for _, col := range msg.Columns {
		if col.Name == p.column {
			if msg.Meta == nil {
				msg.Meta = make(map[string]string)
			}
			if col.Value != nil {
				msg.Meta[p.metaKey] = fmt.Sprintf("%v", col.Value)
			}
			return msg
		}
	}
	return msg
}
