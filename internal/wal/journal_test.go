package wal

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func journalMsg(table, action string, lsn uint64) *Message {
	return &Message{
		Table:  table,
		Action: action,
		LSN:    lsn,
		Meta:   map[string]string{"env": "test"},
	}
}

func TestJournal_NilMessageIsIgnored(t *testing.T) {
	j := NewJournal()
	j.Record(nil)
	if j.Len() != 0 {
		t.Fatalf("expected 0 entries, got %d", j.Len())
	}
}

func TestJournal_RecordIncreasesLen(t *testing.T) {
	j := NewJournal()
	j.Record(journalMsg("orders", "INSERT", 100))
	j.Record(journalMsg("orders", "UPDATE", 200))
	if j.Len() != 2 {
		t.Fatalf("expected 2, got %d", j.Len())
	}
}

func TestJournal_EntriesAreImmutableSnapshot(t *testing.T) {
	j := NewJournal()
	j.Record(journalMsg("users", "DELETE", 42))

	snap := j.Entries()
	if len(snap) != 1 {
		t.Fatalf("expected 1 entry")
	}
	// mutate snapshot — journal must be unaffected
	snap[0].Table = "mutated"
	if j.Entries()[0].Table != "users" {
		t.Errorf("journal entry was mutated through snapshot")
	}
}

func TestJournal_FlushWritesAndClears(t *testing.T) {
	fixed := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	j := NewJournal()
	j.clock = func() time.Time { return fixed }

	j.Record(journalMsg("payments", "INSERT", 999))

	var buf bytes.Buffer
	if err := j.Flush(&buf); err != nil {
		t.Fatalf("flush error: %v", err)
	}

	if j.Len() != 0 {
		t.Errorf("expected journal to be empty after flush")
	}

	out := buf.String()
	if !strings.Contains(out, "payments") {
		t.Errorf("expected 'payments' in output, got: %s", out)
	}
	if !strings.Contains(out, "INSERT") {
		t.Errorf("expected 'INSERT' in output, got: %s", out)
	}
	if !strings.Contains(out, "lsn=999") {
		t.Errorf("expected 'lsn=999' in output, got: %s", out)
	}
}

func TestJournal_MetaIsCopiedNotShared(t *testing.T) {
	msg := journalMsg("tbl", "INSERT", 1)
	j := NewJournal()
	j.Record(msg)

	// mutate original meta after recording
	msg.Meta["env"] = "mutated"

	entries := j.Entries()
	if entries[0].Meta["env"] != "test" {
		t.Errorf("meta was not deep-copied on Record")
	}
}
