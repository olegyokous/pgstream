package wal

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// JournalEntry records a single processed message event.
type JournalEntry struct {
	Timestamp time.Time
	Table     string
	Action    string
	LSN       uint64
	Meta      map[string]string
}

// Journal accumulates JournalEntry records in memory and can flush them to a
// writer. It is safe for concurrent use.
type Journal struct {
	mu      sync.Mutex
	entries []JournalEntry
	clock   func() time.Time
}

// NewJournal returns a Journal ready for use.
func NewJournal() *Journal {
	return &Journal{clock: time.Now}
}

func withJournalClock(fn func() time.Time) func(*Journal) {
	return func(j *Journal) { j.clock = fn }
}

// Record appends an entry derived from msg to the journal.
// Nil messages are silently ignored.
func (j *Journal) Record(msg *Message) {
	if msg == nil {
		return
	}
	e := JournalEntry{
		Timestamp: j.clock(),
		Table:     msg.Table,
		Action:    msg.Action,
		LSN:       msg.LSN,
	}
	if msg.Meta != nil {
		e.Meta = make(map[string]string, len(msg.Meta))
		for k, v := range msg.Meta {
			e.Meta[k] = v
		}
	}
	j.mu.Lock()
	j.entries = append(j.entries, e)
	j.mu.Unlock()
}

// Len returns the number of recorded entries.
func (j *Journal) Len() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.entries)
}

// Flush writes all entries to w in a human-readable format and clears the
// internal buffer.
func (j *Journal) Flush(w io.Writer) error {
	j.mu.Lock()
	entries := j.entries
	j.entries = nil
	j.mu.Unlock()

	for _, e := range entries {
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\tlsn=%d\n",
			e.Timestamp.Format(time.RFC3339Nano), e.Table, e.Action, e.LSN); err != nil {
			return err
		}
	}
	return nil
}

// Entries returns a snapshot of all recorded entries without clearing them.
func (j *Journal) Entries() []JournalEntry {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]JournalEntry, len(j.entries))
	copy(out, j.entries)
	return out
}
