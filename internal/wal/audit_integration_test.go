package wal

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestAuditor_ConcurrentRecords(t *testing.T) {
	var buf safeBuffer
	a := NewAuditor(AuditConfig{Writer: &buf})

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			msg := auditMsg("orders", "INSERT", fmt.Sprintf("0/%d", i))
			if err := a.Record(msg); err != nil {
				t.Errorf("goroutine %d: unexpected error: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != goroutines {
		t.Errorf("expected %d audit lines, got %d", goroutines, len(lines))
	}
}

func TestAuditor_MultipleTablesSelective(t *testing.T) {
	var buf bytes.Buffer
	a := NewAuditor(AuditConfig{
		Tables: []string{"payments"},
		Writer: &buf,
	})

	msgs := []*Message{
		auditMsg("orders", "INSERT", "0/1"),
		auditMsg("payments", "INSERT", "0/2"),
		auditMsg("users", "UPDATE", "0/3"),
		auditMsg("payments", "DELETE", "0/4"),
	}
	for _, m := range msgs {
		_ = a.Record(m)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Errorf("expected 2 audit lines for payments, got %d: %s", len(lines), buf.String())
	}
	for _, l := range lines {
		if !strings.Contains(l, "table=payments") {
			t.Errorf("expected only payments entries, got: %s", l)
		}
	}
}

// TestAuditor_EmptyTableFilter verifies that when no table filter is configured,
// all messages are recorded regardless of their table name.
func TestAuditor_EmptyTableFilter(t *testing.T) {
	var buf bytes.Buffer
	a := NewAuditor(AuditConfig{Writer: &buf})

	msgs := []*Message{
		auditMsg("orders", "INSERT", "0/1"),
		auditMsg("payments", "UPDATE", "0/2"),
		auditMsg("users", "DELETE", "0/3"),
	}
	for _, m := range msgs {
		if err := a.Record(m); err != nil {
			t.Fatalf("unexpected error recording message: %v", err)
		}
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != len(msgs) {
		t.Errorf("expected %d audit lines, got %d: %s", len(msgs), len(lines), buf.String())
	}
}

// safeBuffer is a thread-safe bytes.Buffer.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}
