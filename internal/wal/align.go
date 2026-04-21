package wal

import "fmt"

// Aligner ensures messages from a given table arrive in LSN order,
// dropping any message whose LSN is older than the last seen LSN for
// that table. This is useful when multiple replication slots or
// parallel readers may deliver events out of order.
type Aligner struct {
	lastLSN map[string]uint64
}

// NewAligner returns a new Aligner with an empty LSN state.
func NewAligner() *Aligner {
	return &Aligner{lastLSN: make(map[string]uint64)}
}

// Align returns the message unchanged when its LSN is greater than or
// equal to the last seen LSN for the message's table. If the message
// is out-of-order (older LSN) it returns nil, indicating the caller
// should drop it. A nil message is returned as-is.
func (a *Aligner) Align(msg *Message) (*Message, error) {
	if msg == nil {
		return nil, nil
	}

	key := msg.Table
	if key == "" {
		return msg, nil
	}

	last, ok := a.lastLSN[key]
	if ok && msg.LSN < last {
		return nil, nil
	}

	if msg.LSN > last {
		a.lastLSN[key] = msg.LSN
	}

	return msg, nil
}

// Reset clears the LSN state for all tables, or for a specific table
// when a non-empty table name is provided.
func (a *Aligner) Reset(table string) error {
	if table == "" {
		a.lastLSN = make(map[string]uint64)
		return nil
	}
	if _, ok := a.lastLSN[table]; !ok {
		return fmt.Errorf("aligner: unknown table %q", table)
	}
	delete(a.lastLSN, table)
	return nil
}

// LastLSN returns the highest LSN seen for the given table, and false
// if the table has not been observed yet.
func (a *Aligner) LastLSN(table string) (uint64, bool) {
	v, ok := a.lastLSN[table]
	return v, ok
}

// Tables returns a list of all table names currently tracked by the Aligner.
func (a *Aligner) Tables() []string {
	tables := make([]string, 0, len(a.lastLSN))
	for t := range a.lastLSN {
		tables = append(tables, t)
	}
	return tables
}
