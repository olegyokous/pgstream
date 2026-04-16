package wal

import "fmt"

// Clamper constrains numeric column values to a [Min, Max] range.
// Values below Min are set to Min; values above Max are set to Max.
type Clamper struct {
	rules []ClampRule
}

// ClampRule defines a column and its allowed numeric range.
type ClampRule struct {
	Table  string
	Column string
	Min    float64
	Max    float64
}

// NewClamper returns a Clamper that applies the given rules.
// At least one rule must be provided and Min must be <= Max.
func NewClamper(rules []ClampRule) (*Clamper, error) {
	if len(rules) == 0 {
		return nil, fmt.Errorf("clamper: at least one rule required")
	}
	for _, r := range rules {
		if r.Min > r.Max {
			return nil, fmt.Errorf("clamper: rule for column %q has Min(%v) > Max(%v)", r.Column, r.Min, r.Max)
		}
		if r.Column == "" {
			return nil, fmt.Errorf("clamper: rule has empty column name")
		}
	}
	return &Clamper{rules: rules}, nil
}

// Apply clamps matching numeric columns in msg according to the configured rules.
// Non-numeric or non-matching columns are left unchanged.
// A nil message is returned as-is.
func (c *Clamper) Apply(msg *Message) (*Message, error) {
	if msg == nil {
		return nil, nil
	}
	for _, rule := range c.rules {
		if rule.Table != "" && rule.Table != msg.Table {
			continue
		}
		for i, col := range msg.Columns {
			if col.Name != rule.Column {
				continue
			}
			switch v := col.Value.(type) {
			case float64:
				msg.Columns[i].Value = clampFloat(v, rule.Min, rule.Max)
			case int:
				msg.Columns[i].Value = int(clampFloat(float64(v), rule.Min, rule.Max))
			case int64:
				msg.Columns[i].Value = int64(clampFloat(float64(v), rule.Min, rule.Max))
			}
		}
	}
	return msg, nil
}

func clampFloat(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
