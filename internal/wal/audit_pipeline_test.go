package wal

import (
	"bytes"
	"strings"
	"testing"
)

// TestAuditor_IntegratesWithFilter verifies that the Auditor can sit alongside
// the Filter in a simple pipeline: filter first, then audit survivors.
func TestAuditor_IntegratesWithFilter(t *testing.T) {
	var buf bytes.Buffer
	auditor := NewAuditor(AuditConfig{
		Actions: []string{"DELETE"},
		Writer:  &buf,
	})
	filter := NewFilter(FilterConfig{
		Tables:  []string{"orders"},
	})

	msgs := []*Message{
		{Table: "orders", Action: "INSERT", LSN: "0/1"},
		{Table: "orders", Action: "DELETE", LSN: "0/2"},
		{Table: "users", Action: "DELETE", LSN: "0/3"},
	}

	for _, m := range msgs {
		if filter.Allow(m) {
			_ = auditor.Record(m)
		}
	}

	// Only orders/DELETE should pass both filter and auditor.
	if !strings.Contains(buf.String(), "lsn=0/2") {
		t.Errorf("expected lsn=0/2 in audit log, got: %s", buf.String())
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Errorf("expected exactly 1 audit entry, got %d", len(lines))
	}
}

// TestAuditor_RecordsLSNInOutput checks that LSN values appear in the log.
func TestAuditor_RecordsLSNInOutput(t *testing.T) {
	var buf bytes.Buffer
	a := NewAuditor(AuditConfig{Writer: &buf})

	_ = a.Record(auditMsg("inventory", "UPDATE", "A/BCDEF012"))

	if !strings.Contains(buf.String(), "lsn=A/BCDEF012") {
		t.Errorf("expected lsn in audit output, got: %s", buf.String())
	}
}
