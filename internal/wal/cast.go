package wal

import (
	"fmt"
	"strconv"
	"strings"
)

// CastRule defines a type coercion for a specific table column.
type CastRule struct {
	Table  string
	Column string
	Type   string // "int", "float", "bool", "string"
}

// Caster coerces column values to target types based on rules.
type Caster struct {
	rules []CastRule
}

// NewCaster returns a Caster that applies the given coercion rules.
// Returns an error if no rules are provided.
func NewCaster(rules []CastRule) (*Caster, error) {
	if len(rules) == 0 {
		return nil, fmt.Errorf("cast: at least one rule is required")
	}
	for _, r := range rules {
		switch r.Type {
		case "int", "float", "bool", "string":
		default:
			return nil, fmt.Errorf("cast: unsupported type %q for column %q", r.Type, r.Column)
		}
	}
	return &Caster{rules: rules}, nil
}

// Apply coerces matching column values in msg according to the registered rules.
// Returns the (possibly modified) message and any coercion error.
func (c *Caster) Apply(msg *Message) (*Message, error) {
	if msg == nil {
		return nil, nil
	}
	for _, rule := range c.rules {
		if rule.Table != "" && !strings.EqualFold(msg.Table, rule.Table) {
			continue
		}
		for i, col := range msg.Columns {
			if !strings.EqualFold(col.Name, rule.Column) {
				continue
			}
			raw, ok := col.Value.(string)
			if !ok {
				continue
			}
			coerced, err := coerce(raw, rule.Type)
			if err != nil {
				return msg, fmt.Errorf("cast: column %q value %q: %w", col.Name, raw, err)
			}
			msg.Columns[i].Value = coerced
		}
	}
	return msg, nil
}

func coerce(raw, typ string) (any, error) {
	switch typ {
	case "int":
		return strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	case "float":
		return strconv.ParseFloat(strings.TrimSpace(raw), 64)
	case "bool":
		return strconv.ParseBool(strings.TrimSpace(raw))
	case "string":
		return raw, nil
	default:
		return nil, fmt.Errorf("unknown type %q", typ)
	}
}
