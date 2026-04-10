package wal

import "time"

// ActionType represents the type of WAL change.
type ActionType string

const (
	ActionInsert   ActionType = "INSERT"
	ActionUpdate   ActionType = "UPDATE"
	ActionDelete   ActionType = "DELETE"
	ActionTruncate ActionType = "TRUNCATE"
)

// Column holds the name, type, and value of a table column.
type Column struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

// Message represents a decoded WAL logical replication message.
type Message struct {
	LSN       string     `json:"lsn"`
	Timestamp time.Time  `json:"timestamp"`
	Action    ActionType `json:"action"`
	Schema    string     `json:"schema"`
	Table     string     `json:"table"`
	Columns   []Column   `json:"columns,omitempty"`
	OldKeys   []Column   `json:"old_keys,omitempty"`
}

// IsDataChange reports whether the message represents a data modification
// (INSERT, UPDATE, or DELETE), as opposed to a structural change like TRUNCATE.
func (m *Message) IsDataChange() bool {
	switch m.Action {
	case ActionInsert, ActionUpdate, ActionDelete:
		return true
	default:
		return false
	}
}

// GetColumn returns the column with the given name, or false if not found.
func (m *Message) GetColumn(name string) (Column, bool) {
	for _, col := range m.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return Column{}, false
}

// Relation holds metadata about a PostgreSQL table.
type Relation struct {
	ID      uint32
	Schema  string
	Table   string
	Columns []RelationColumn
}

// RelationColumn holds column metadata from a relation message.
type RelationColumn struct {
	Name string
	Type uint32
}
