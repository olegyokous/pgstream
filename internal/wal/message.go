package wal

import "time"

// ActionType represents the type of WAL change.
type ActionType string

const (
	ActionInsert ActionType = "INSERT"
	ActionUpdate ActionType = "UPDATE"
	ActionDelete ActionType = "DELETE"
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
