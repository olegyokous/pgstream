package wal

import "fmt"

// Summary holds aggregated counts of WAL messages grouped by table and action.
type Summary struct {
	Counts map[string]map[string]int // table -> action -> count
	Total  int
}

// String returns a human-readable representation of the summary.
func (s Summary) String() string {
	if s.Total == 0 {
		return "summary: no messages"
	}
	out := fmt.Sprintf("summary: total=%d\n", s.Total)
	for table, actions := range s.Counts {
		for action, count := range actions {
			out += fmt.Sprintf("  %s.%s: %d\n", table, action, count)
		}
	}
	return out
}

// SummarizerOption configures a Summarizer.
type SummarizerOption func(*Summarizer)

// WithSummarizerTable restricts the summarizer to a specific table.
func WithSummarizerTable(table string) SummarizerOption {
	return func(s *Summarizer) { s.table = table }
}

// Summarizer accumulates message counts and emits a Summary on demand.
type Summarizer struct {
	table  string
	counts map[string]map[string]int
	total  int
}

// NewSummarizer returns a new Summarizer.
func NewSummarizer(opts ...SummarizerOption) *Summarizer {
	s := &Summarizer{counts: make(map[string]map[string]int)}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Record accumulates the message into the summary.
// Returns the message unchanged to allow chaining.
func (s *Summarizer) Record(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	if s.table != "" && msg.Table != s.table {
		return msg
	}
	if s.counts[msg.Table] == nil {
		s.counts[msg.Table] = make(map[string]int)
	}
	s.counts[msg.Table][msg.Action]++
	s.total++
	return msg
}

// Snapshot returns an immutable copy of the current summary.
func (s *Summarizer) Snapshot() Summary {
	copy := make(map[string]map[string]int, len(s.counts))
	for t, actions := range s.counts {
		am := make(map[string]int, len(actions))
		for a, c := range actions {
			am[a] = c
		}
		copy[t] = am
	}
	return Summary{Counts: copy, Total: s.total}
}

// Reset clears all accumulated counts.
func (s *Summarizer) Reset() {
	s.counts = make(map[string]map[string]int)
	s.total = 0
}
