package wal

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func observeMsg() *Message {
	return &Message{
		LSN:    42,
		Action: "INSERT",
		Table:  "orders",
		Columns: []Column{
			{Name: "id", Value: "1"},
			{Name: "total", Value: "99.99"},
		},
	}
}

func TestObserver_NilMessagePassthrough(t *testing.T) {
	buf := &bytes.Buffer{}
	o := NewObserver(ObserverConfig{Output: buf})
	got := o.Observe(nil)
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
	if buf.Len() != 0 {
		t.Errorf("expected no output for nil message, got %q", buf.String())
	}
}

func TestObserver_ReturnsMessageUnchanged(t *testing.T) {
	buf := &bytes.Buffer{}
	o := NewObserver(ObserverConfig{Output: buf})
	msg := observeMsg()
	got := o.Observe(msg)
	if got != msg {
		t.Error("expected the same message pointer to be returned")
	}
}

func TestObserver_OutputContainsFields(t *testing.T) {
	buf := &bytes.Buffer{}
	o := NewObserver(ObserverConfig{Output: buf})
	o.Observe(observeMsg())
	out := buf.String()
	for _, want := range []string{"INSERT", "orders", "42", "2"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected output to contain %q, got: %s", want, out)
		}
	}
}

func TestObserver_PrefixAppearsInOutput(t *testing.T) {
	buf := &bytes.Buffer{}
	o := NewObserver(ObserverConfig{Output: buf, Prefix: "[debug]"})
	o.Observe(observeMsg())
	if !strings.HasPrefix(buf.String(), "[debug] ") {
		t.Errorf("expected output to start with prefix, got: %s", buf.String())
	}
}

func TestObserver_DefaultsApplied(t *testing.T) {
	// NewObserver with empty config should not panic and use stderr (non-nil output)
	o := NewObserver(ObserverConfig{})
	if o.cfg.Output == nil {
		t.Error("expected non-nil default output")
	}
	if o.cfg.TimestampFormat == "" {
		t.Error("expected non-empty default timestamp format")
	}
}

func TestObserver_TimestampFormat(t *testing.T) {
	buf := &bytes.Buffer{}
	fixedNow := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	o := NewObserver(ObserverConfig{
		Output:          buf,
		TimestampFormat: "2006-01-02",
	})
	o.clock = func() time.Time { return fixedNow }
	o.Observe(observeMsg())
	if !strings.Contains(buf.String(), "2024-06-01") {
		t.Errorf("expected formatted timestamp in output, got: %s", buf.String())
	}
}
