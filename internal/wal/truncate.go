package wal

import "fmt"

// Truncator drops or passes through messages whose payload exceeds a
// configurable byte threshold. Oversized messages can either be dropped
// silently or replaced with a sentinel stub so downstream sinks are never
// handed arbitrarily large payloads.
type Truncator struct {
	maxBytes int
	stubOnExceed bool
}

// DefaultTruncatorMaxBytes is 1 MiB.
const DefaultTruncatorMaxBytes = 1 << 20

// NewTruncator returns a Truncator that drops messages whose formatted
// payload exceeds maxBytes. When stubOnExceed is true the message is kept
// but its columns are replaced with a single "_truncated" marker column.
func NewTruncator(maxBytes int, stubOnExceed bool) (*Truncator, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("truncator: maxBytes must be positive, got %d", maxBytes)
	}
	return &Truncator{maxBytes: maxBytes, stubOnExceed: stubOnExceed}, nil
}

// Apply evaluates msg against the byte threshold and returns the (possibly
// mutated) message together with a boolean indicating whether the caller
// should forward it. A nil message is passed through unchanged.
func (t *Truncator) Apply(msg *Message) (*Message, bool) {
	if msg == nil {
		return nil, true
	}

	size := messageSize(msg)
	if size <= t.maxBytes {
		return msg, true
	}

	if !t.stubOnExceed {
		return nil, false
	}

	// Replace columns with a single stub so the message stays routable.
	stub := *msg
	stub.Columns = []Column{{Name: "_truncated", Value: fmt.Sprintf("original_bytes=%d", size)}}
	return &stub, true
}

// messageSize returns a rough byte estimate for a Message.
func messageSize(msg *Message) int {
	n := len(msg.Table) + len(msg.Schema) + len(msg.Action)
	for _, c := range msg.Columns {
		n += len(c.Name)
		if s, ok := c.Value.(string); ok {
			n += len(s)
		} else {
			n += 8 // rough estimate for numeric / bool types
		}
	}
	return n
}
