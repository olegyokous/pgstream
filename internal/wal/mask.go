package wal

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

// MaskMode controls how a masked value is rendered.
type MaskMode string

const (
	MaskModeRedact MaskMode = "redact"  // replace with fixed string
	MaskModeHash   MaskMode = "hash"    // replace with SHA-256 hex digest
	MaskModeBlank  MaskMode = "blank"   // replace with empty string
)

// MaskRule describes which table/column to mask and how.
type MaskRule struct {
	Table   string
	Column  string
	Mode    MaskMode
	Replace string // used when Mode == MaskModeRedact; defaults to "***"
}

// Masker applies column-level masking rules to WAL messages.
type Masker struct {
	rules []MaskRule
}

// NewMasker returns a Masker that applies the given rules.
// Returns an error if no rules are provided.
func NewMasker(rules []MaskRule) (*Masker, error) {
	if len(rules) == 0 {
		return nil, fmt.Errorf("masker: at least one rule is required")
	}
	return &Masker{rules: rules}, nil
}

// Apply processes msg, masking column values that match any rule.
// A nil message is returned unchanged.
func (m *Masker) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	for i, col := range msg.Columns {
		for _, r := range m.rules {
			if !tableMatches(r.Table, msg.Table) {
				continue
			}
			if !strings.EqualFold(r.Column, col.Name) {
				continue
			}
			msg.Columns[i].Value = maskValue(col.Value, r)
		}
	}
	return msg
}

func tableMatches(pattern, table string) bool {
	if pattern == "" || pattern == "*" {
		return true
	}
	return strings.EqualFold(pattern, table)
}

func maskValue(v interface{}, r MaskRule) interface{} {
	switch r.Mode {
	case MaskModeHash:
		s := fmt.Sprintf("%v", v)
		sum := sha256.Sum256([]byte(s))
		return fmt.Sprintf("%x", sum)
	case MaskModeBlank:
		return ""
	default: // MaskModeRedact
		if r.Replace != "" {
			return r.Replace
		}
		return "***"
	}
}
