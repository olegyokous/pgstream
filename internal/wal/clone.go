package wal

import "fmt"

// Cloner produces a deep copy of a Message so that downstream stages can
// mutate their copy without affecting other branches of the pipeline.
type Cloner struct {
	table  string
	action string
}

// CloneOption configures a Cloner.
type CloneOption func(*Cloner)

// WithCloneTable restricts cloning to messages whose table matches.
func WithCloneTable(table string) CloneOption {
	return func(c *Cloner) { c.table = table }
}

// WithCloneAction restricts cloning to messages whose action matches.
func WithCloneAction(action string) CloneOption {
	return func(c *Cloner) { c.action = action }
}

// NewCloner returns a Cloner configured with the supplied options.
func NewCloner(opts ...CloneOption) *Cloner {
	c := &Cloner{}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Clone returns a deep copy of msg when it matches the configured constraints.
// If msg is nil the function returns nil, nil.
// If the message does not match the constraints the original pointer is
// returned unchanged (no copy is made).
func (c *Cloner) Clone(msg *Message) (*Message, error) {
	if msg == nil {
		return nil, nil
	}
	if c.table != "" && msg.Table != c.table {
		return msg, nil
	}
	if c.action != "" && msg.Action != c.action {
		return msg, nil
	}
	return cloneMessage(msg)
}

func cloneMessage(src *Message) (*Message, error) {
	if src == nil {
		return nil, fmt.Errorf("clone: source message is nil")
	}
	dst := &Message{
		LSN:    src.LSN,
		Table:  src.Table,
		Action: src.Action,
	}
	if src.Columns != nil {
		dst.Columns = make(map[string]any, len(src.Columns))
		for k, v := range src.Columns {
			dst.Columns[k] = v
		}
	}
	if src.Meta != nil {
		dst.Meta = make(map[string]string, len(src.Meta))
		for k, v := range src.Meta {
			dst.Meta[k] = v
		}
	}
	return dst, nil
}
