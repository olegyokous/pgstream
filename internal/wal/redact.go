package wal

import (
	"regexp"
	"strings"
)

// RedactConfig holds configuration for the Redactor.
type RedactConfig struct {
	// Columns is a map of table name to column names whose values should be redacted.
	Columns map[string][]string
	// Replacement is the string used in place of redacted values. Defaults to "[REDACTED]".
	Replacement string
	// Pattern, if set, redacts any column value matching this regex regardless of column name.
	Pattern string
}

// Redactor replaces sensitive column values in WAL messages.
type Redactor struct {
	cfg     RedactConfig
	pattern *regexp.Regexp
}

// NewRedactor creates a Redactor from cfg.
// Returns an error if cfg.Pattern is an invalid regular expression.
func NewRedactor(cfg RedactConfig) (*Redactor, error) {
	if cfg.Replacement == "" {
		cfg.Replacement = "[REDACTED]"
	}
	var re *regexp.Regexp
	if cfg.Pattern != "" {
		var err error
		re, err = regexp.Compile(cfg.Pattern)
		if err != nil {
			return nil, err
		}
	}
	return &Redactor{cfg: cfg, pattern: re}, nil
}

// Apply returns a copy of msg with sensitive columns redacted.
func (r *Redactor) Apply(msg Message) Message {
	if len(msg.Columns) == 0 {
		return msg
	}
	redactSet := r.redactSet(msg.Table)
	out := make([]Column, len(msg.Columns))
	copy(out, msg.Columns)
	for i, col := range out {
		val, ok := col.Value.(string)
		if !ok {
			continue
		}
		if redactSet[col.Name] {
			out[i].Value = r.cfg.Replacement
			continue
		}
		if r.pattern != nil && r.pattern.MatchString(val) {
			out[i].Value = r.cfg.Replacement
		}
	}
	msg.Columns = out
	return msg
}

// redactSet returns a set of column names to redact for the given table.
func (r *Redactor) redactSet(table string) map[string]bool {
	set := make(map[string]bool)
	for _, col := range r.cfg.Columns[table] {
		set[strings.ToLower(col)] = true
	}
	return set
}
