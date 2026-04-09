package wal

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewFormatter_UnknownFormat(t *testing.T) {
	_, err := NewFormatter("xml")
	if err == nil {
		t.Fatal("expected error for unknown format, got nil")
	}
}

func TestNewFormatter_KnownFormats(t *testing.T) {
	for _, f := range []Format{FormatJSON, FormatText, FormatPretty} {
		if _, err := NewFormatter(f); err != nil {
			t.Errorf("NewFormatter(%q) unexpected error: %v", f, err)
		}
	}
}

func sampleMsg() *Message {
	return &Message{
		Action: "INSERT",
		Schema: "public",
		Table:  "users",
		Columns: []Column{
			{Name: "id", Value: float64(1)},
			{Name: "name", Value: "alice"},
		},
	}
}

func TestJSONFormatter(t *testing.T) {
	f, _ := NewFormatter(FormatJSON)
	out, err := f.Format(sampleMsg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var m Message
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if m.Table != "users" {
		t.Errorf("expected table=users, got %q", m.Table)
	}
}

func TestTextFormatter(t *testing.T) {
	f, _ := NewFormatter(FormatText)
	out, err := f.Format(sampleMsg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(out, "INSERT public.users") {
		t.Errorf("unexpected text output: %q", out)
	}
	if !strings.Contains(out, "id=1") {
		t.Errorf("expected id=1 in output, got %q", out)
	}
}

func TestPrettyFormatter(t *testing.T) {
	f, _ := NewFormatter(FormatPretty)
	out, err := f.Format(sampleMsg())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "\n") {
		t.Errorf("expected indented output, got single line: %q", out)
	}
	var m Message
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
}
