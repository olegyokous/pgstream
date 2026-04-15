package wal

import "fmt"

// MergeStrategy controls how column conflicts are resolved when merging two messages.
type MergeStrategy int

const (
	// MergePreferSrc overwrites destination columns with source values.
	MergePreferSrc MergeStrategy = iota
	// MergePreferDst keeps destination columns when a conflict occurs.
	MergePreferDst
)

// MergerOption configures a Merger.
type MergerOption func(*Merger)

// WithMergeStrategy sets the conflict resolution strategy.
func WithMergeStrategy(s MergeStrategy) MergerOption {
	return func(m *Merger) { m.strategy = s }
}

// WithMergeTable restricts merging to messages from the named table.
func WithMergeTable(table string) MergerOption {
	return func(m *Merger) { m.table = table }
}

// Merger combines columns from a source message into a destination message.
type Merger struct {
	strategy MergeStrategy
	table    string
}

// NewMerger constructs a Merger with the given options.
func NewMerger(opts ...MergerOption) (*Merger, error) {
	m := &Merger{strategy: MergePreferSrc}
	for _, o := range opts {
		o(m)
	}
	return m, nil
}

// Merge combines src columns into dst, returning the updated dst.
// If a table constraint is set, both messages must belong to that table.
// Nil src returns dst unchanged; nil dst returns an error.
func (m *Merger) Merge(dst, src *Message) (*Message, error) {
	if dst == nil {
		return nil, fmt.Errorf("merge: dst message must not be nil")
	}
	if src == nil {
		return dst, nil
	}
	if m.table != "" && (dst.Table != m.table || src.Table != m.table) {
		return dst, nil
	}
	if dst.Columns == nil {
		dst.Columns = make([]Column, 0, len(src.Columns))
	}
	existing := make(map[string]int, len(dst.Columns))
	for i, c := range dst.Columns {
		existing[c.Name] = i
	}
	for _, sc := range src.Columns {
		if idx, ok := existing[sc.Name]; ok {
			if m.strategy == MergePreferSrc {
				dst.Columns[idx] = sc
			}
		} else {
			dst.Columns = append(dst.Columns, sc)
		}
	}
	return dst, nil
}
