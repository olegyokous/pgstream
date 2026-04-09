package wal

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Format defines the output format for WAL messages.
type Format string

const (
	FormatJSON   Format = "json"
	FormatText   Format = "text"
	FormatPretty Format = "pretty"
)

// Formatter converts a Message into a string representation.
type Formatter interface {
	Format(msg *Message) (string, error)
}

// NewFormatter returns a Formatter for the given format string.
// It returns an error if the format is unrecognised.
func NewFormatter(f Format) (Formatter, error) {
	switch f {
	case FormatJSON:
		return &jsonFormatter{}, nil
	case FormatText:
		return &textFormatter{}, nil
	case FormatPretty:
		return &prettyFormatter{}, nil
	default:
		return nil, fmt.Errorf("unknown format %q: must be one of json, text, pretty", f)
	}
}

// jsonFormatter emits compact JSON.
type jsonFormatter struct{}

func (j *jsonFormatter) Format(msg *Message) (string, error) {
	b, err := json.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("json format: %w", err)
	}
	return string(b), nil
}

// textFormatter emits a simple human-readable line.
type textFormatter struct{}

func (t *textFormatter) Format(msg *Message) (string, error) {
	pairs := make([]string, 0, len(msg.Columns))
	for _, c := range msg.Columns {
		pairs = append(pairs, fmt.Sprintf("%s=%v", c.Name, c.Value))
	}
	return fmt.Sprintf("%s %s.%s %s", msg.Action, msg.Schema, msg.Table, strings.Join(pairs, " ")), nil
}

// prettyFormatter emits indented JSON.
type prettyFormatter struct{}

func (p *prettyFormatter) Format(msg *Message) (string, error) {
	b, err := json.MarshalIndent(msg, "", "  ")
	if err != nil {
		return "", fmt.Errorf("pretty format: %w", err)
	}
	return string(b), nil
}
