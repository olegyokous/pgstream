package wal

import (
	"fmt"
	"strings"
	"testing"
)

func maskMsg(table, col string, val interface{}) *Message {
	return &Message{
		Table:  table,
		Action: "INSERT",
		Columns: []Column{
			{Name: col, Value: val},
		},
	}
}

func TestNewMasker_NoRulesErrors(t *testing.T) {
	_, err := NewMasker(nil)
	if err == nil {
		t.Fatal("expected error for empty rules")
	}
}

func TestMasker_NilMessagePassthrough(t *testing.T) {
	m, _ := NewMasker([]MaskRule{{Table: "users", Column: "email", Mode: MaskModeRedact}})
	if got := m.Apply(nil); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestMasker_RedactDefaultReplacement(t *testing.T) {
	m, _ := NewMasker([]MaskRule{{Table: "users", Column: "email", Mode: MaskModeRedact}})
	msg := maskMsg("users", "email", "alice@example.com")
	out := m.Apply(msg)
	if out.Columns[0].Value != "***" {
		t.Errorf("expected ***, got %v", out.Columns[0].Value)
	}
}

func TestMasker_RedactCustomReplacement(t *testing.T) {
	m, _ := NewMasker([]MaskRule{{Table: "users", Column: "email", Mode: MaskModeRedact, Replace: "[hidden]"}})
	msg := maskMsg("users", "email", "alice@example.com")
	out := m.Apply(msg)
	if out.Columns[0].Value != "[hidden]" {
		t.Errorf("expected [hidden], got %v", out.Columns[0].Value)
	}
}

func TestMasker_HashProducesSHA256Hex(t *testing.T) {
	m, _ := NewMasker([]MaskRule{{Table: "*", Column: "ssn", Mode: MaskModeHash}})
	msg := maskMsg("customers", "ssn", "123-45-6789")
	out := m.Apply(msg)
	hashed, ok := out.Columns[0].Value.(string)
	if !ok || len(hashed) != 64 {
		t.Errorf("expected 64-char hex hash, got %v", out.Columns[0].Value)
	}
}

func TestMasker_BlankEmptiesValue(t *testing.T) {
	m, _ := NewMasker([]MaskRule{{Table: "orders", Column: "card", Mode: MaskModeBlank}})
	msg := maskMsg("orders", "card", "4111111111111111")
	out := m.Apply(msg)
	if out.Columns[0].Value != "" {
		t.Errorf("expected empty string, got %v", out.Columns[0].Value)
	}
}

func TestMasker_NonMatchingTableUnchanged(t *testing.T) {
	m, _ := NewMasker([]MaskRule{{Table: "users", Column: "email", Mode: MaskModeRedact}})
	msg := maskMsg("orders", "email", "bob@example.com")
	out := m.Apply(msg)
	if out.Columns[0].Value != "bob@example.com" {
		t.Errorf("expected original value, got %v", out.Columns[0].Value)
	}
}

func TestMasker_CaseInsensitiveColumnMatch(t *testing.T) {
	m, _ := NewMasker([]MaskRule{{Table: "users", Column: "Email", Mode: MaskModeRedact}})
	msg := maskMsg("users", "email", "test@test.com")
	out := m.Apply(msg)
	if out.Columns[0].Value != "***" {
		t.Errorf("expected ***, got %v", out.Columns[0].Value)
	}
}

func TestMasker_WildcardTableMatchesAll(t *testing.T) {
	m, _ := NewMasker([]MaskRule{{Table: "*", Column: "secret", Mode: MaskModeBlank}})
	tables := []string{"users", "orders", "events"}
	for _, tbl := range tables {
		msg := maskMsg(tbl, "secret", "top-secret")
		out := m.Apply(msg)
		if fmt.Sprintf("%v", out.Columns[0].Value) != "" {
			t.Errorf("table %s: expected blank, got %v", tbl, out.Columns[0].Value)
		}
	}
	_ = strings.ToLower
}
