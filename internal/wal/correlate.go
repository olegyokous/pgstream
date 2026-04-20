package wal

import "fmt"

// Correlater links messages together by a shared correlation ID derived from
// a column value or meta key, stamping the result into message metadata.
type Correlater struct {
	column  string
	metaKey string
	table   string
}

// WithCorrelaterTable scopes the correlater to a specific table.
func WithCorrelaterTable(table string) func(*Correlater) {
	return func(c *Correlater) {
		c.table = table
	}
}

// NewCorrelater creates a Correlater that reads the given column and writes
// its value into metaKey on the message metadata map.
func NewCorrelater(column, metaKey string, opts ...func(*Correlater)) (*Correlater, error) {
	if column == "" {
		return nil, fmt.Errorf("correlater: column must not be empty")
	}
	if metaKey == "" {
		return nil, fmt.Errorf("correlater: metaKey must not be empty")
	}
	c := &Correlater{column: column, metaKey: metaKey}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// Apply stamps the correlation ID onto msg.Meta and returns the message
// unchanged. Returns nil when msg is nil.
func (c *Correlater) Apply(msg *Message) (*Message, error) {
	if msg == nil {
		return nil, nil
	}
	if c.table != "" && msg.Table != c.table {
		return msg, nil
	}
	var val interface{}
	for _, col := range msg.Columns {
		if col.Name == c.column {
			val = col.Value
			break
		}
	}
	if val == nil {
		return msg, nil
	}
	if msg.Meta == nil {
		msg.Meta = make(map[string]interface{})
	}
	msg.Meta[c.metaKey] = val
	return msg, nil
}
