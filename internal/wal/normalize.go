package wal

import (
	"fmt"
	"strings"
)

// NormalizeFunc transforms a column value into a normalised string form.
type NormalizeFunc func(v interface{}) interface{}

// NormalizerConfig holds configuration for the Normalizer.
type NormalizerConfig struct {
	// Table restricts normalisation to the named table; empty means all tables.
	Table string
	// Columns maps column names to their NormalizeFunc. Columns not present are
	// passed through unchanged.
	Columns map[string]NormalizeFunc
}

// Normalizer applies per-column transformation functions to WAL messages.
type Normalizer struct {
	cfg NormalizerConfig
}

// NewNormalizer returns a Normalizer configured with cfg.
// It returns an error when no column rules are provided.
func NewNormalizer(cfg NormalizerConfig) (*Normalizer, error) {
	if len(cfg.Columns) == 0 {
		return nil, fmt.Errorf("normalizer: at least one column rule is required")
	}
	return &Normalizer{cfg: cfg}, nil
}

// Apply runs the configured normalisation functions over msg.
// If the Normalizer targets a specific table and msg.Table does not match,
// the message is returned unchanged. A nil msg is returned as-is.
func (n *Normalizer) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	if n.cfg.Table != "" && msg.Table != n.cfg.Table {
		return msg
	}
	for i, col := range msg.Columns {
		fn, ok := n.cfg.Columns[col.Name]
		if !ok {
			continue
		}
		msg.Columns[i].Value = fn(col.Value)
	}
	return msg
}

// TrimSpace is a NormalizeFunc that trims leading/trailing whitespace from
// string values; non-string values are returned unchanged.
func TrimSpace(v interface{}) interface{} {
	s, ok := v.(string)
	if !ok {
		return v
	}
	return strings.TrimSpace(s)
}

// ToLower is a NormalizeFunc that lower-cases string values.
func ToLower(v interface{}) interface{} {
	s, ok := v.(string)
	if !ok {
		return v
	}
	return strings.ToLower(s)
}

// ToUpper is a NormalizeFunc that upper-cases string values.
func ToUpper(v interface{}) interface{} {
	s, ok := v.(string)
	if !ok {
		return v
	}
	return strings.ToUpper(s)
}
