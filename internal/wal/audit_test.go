package wal

import (
	"bytes"
	"strings"
	"testing"
)

func auditMsg(table, action, lsn string) *Message {
	return &Message{Table: table, Action: action, LSN: lsn}
}

func TestAuditor_RecordsMatchingMessage(t *testing.T) {
	var buf bytes.Buffer
	a := NewAuditor(AuditConfig{Writer: &buf})

	if err := a.Record(auditMsg("orders", "INSERT", "0/1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(buf.String(), "table=orders") {
		t.Errorf("expected table=orders in output, got: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "action=INSERT") {
		t.Errorf("expected action=INSERT in output, got: %s", buf.String())
	}
}

func TestAuditor_FiltersUnmatchedTable(t *testing.T) {
	var buf bytes.Buffer
	a := NewAuditor(AuditConfig{
		Tables: []string{"orders"},
		Writer: &buf,
	})

	_ = a.Record(auditMsg("users", "INSERT", "0/2"))
	if buf.Len() != 0 {
		t.Errorf("expected no output for unmatched table, got: %s", buf.String())
	}
}

func TestAuditor_FiltersUnmatchedAction(t *testing.T) {
	var buf bytes.Buffer
	a := NewAuditor(AuditConfig{
		Actions: []string{"DELETE"},
		Writer:  &buf,
	})

	_ = a.Record(auditMsg("orders", "INSERT", "0/3"))
	if buf.Len() != 0 {
		t.Errorf("expected no output for unmatched action, got: %s", buf.String())
	}
}

func TestAuditor_TableAndActionBothMustMatch(t *testing.T) {
	var buf bytes.Buffer
	a := NewAuditor(AuditConfig{
		Tables:  []string{"orders"},
		Actions: []string{"DELETE"},
		Writer:  &buf,
	})

	_ = a.Record(auditMsg("orders", "INSERT", "0/4"))
	if buf.Len() != 0 {
		t.Errorf("expected no output when action doesn't match, got: %s", buf.String())
	}

	_ = a.Record(auditMsg("orders", "DELETE", "0/5"))
	if !strings.Contains(buf.String(), "action=DELETE") {
		t.Errorf("expected DELETE to be recorded, got: %s", buf.String())
	}
}

func TestAuditor_DefaultWriterDoesNotPanic(t *testing.T) {
	a := NewAuditor(AuditConfig{})
	if err := a.Record(auditMsg("t", "INSERT", "0/1")); err != nil {
		t.Fatalf("unexpected error with default writer: %v", err)
	}
}
