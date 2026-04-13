package wal

import (
	"strings"
	"testing"
)

// TestSkipper_IntegratesWithFormatter verifies that a Skipper placed before a
// Formatter correctly prevents dropped messages from being formatted.
func TestSkipper_IntegratesWithFormatter(t *testing.T) {
	skipper, err := NewSkipper(WithSkipTables("internal_events"))
	if err != nil {
		t.Fatalf("NewSkipper: %v", err)
	}

	fmt, err := NewFormatter("json")
	if err != nil {
		t.Fatalf("NewFormatter: %v", err)
	}

	process := func(msg *Message) string {
		passed := skipper.Apply(msg)
		if passed == nil {
			return ""
		}
		out, _ := fmt.Format(passed)
		return string(out)
	}

	// skipped table → empty output
	if got := process(skipMsg("internal_events", "INSERT")); got != "" {
		t.Fatalf("expected empty output, got %q", got)
	}

	// allowed table → formatted JSON
	got := process(skipMsg("orders", "INSERT"))
	if !strings.Contains(got, "orders") {
		t.Fatalf("expected formatted output containing 'orders', got %q", got)
	}
}

// TestSkipper_MultipleSkipRulesInChain verifies that several skip rules
// applied in sequence all have a chance to drop a message.
func TestSkipper_MultipleSkipRulesInChain(t *testing.T) {
	s1, _ := NewSkipper(WithSkipTables("audit"))
	s2, _ := NewSkipper(WithSkipActions("DELETE"))

	apply := func(msg *Message) *Message {
		msg = s1.Apply(msg)
		if msg == nil {
			return nil
		}
		return s2.Apply(msg)
	}

	cases := []struct {
		table, action string
		wantDrop      bool
	}{
		{"audit", "INSERT", true},
		{"orders", "DELETE", true},
		{"orders", "INSERT", false},
	}

	for _, tc := range cases {
		got := apply(skipMsg(tc.table, tc.action))
		if tc.wantDrop && got != nil {
			t.Errorf("table=%s action=%s: expected drop, got message", tc.table, tc.action)
		}
		if !tc.wantDrop && got == nil {
			t.Errorf("table=%s action=%s: expected pass, got nil", tc.table, tc.action)
		}
	}
}
