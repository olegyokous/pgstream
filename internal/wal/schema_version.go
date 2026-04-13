package wal

import (
	"fmt"
	"sync"
)

// SchemaVersion tracks the version of a relation's schema by monitoring
// column-set changes over time.
type SchemaVersion struct {
	mu       sync.RWMutex
	versions map[uint32]uint64 // relationID -> version counter
	fingerprints map[uint32]string // relationID -> column fingerprint
}

// NewSchemaVersion creates a new SchemaVersion tracker.
func NewSchemaVersion() *SchemaVersion {
	return &SchemaVersion{
		versions:     make(map[uint32]uint64),
		fingerprints: make(map[uint32]string),
	}
}

// Observe checks whether the relation's schema has changed since the last
// observation. It returns the current version number and whether the schema
// changed (true = new or changed).
func (sv *SchemaVersion) Observe(rel Relation) (version uint64, changed bool) {
	fp := fingerprint(rel)

	sv.mu.Lock()
	defer sv.mu.Unlock()

	prev, ok := sv.fingerprints[rel.ID]
	if !ok || prev != fp {
		sv.versions[rel.ID]++
		sv.fingerprints[rel.ID] = fp
		return sv.versions[rel.ID], true
	}
	return sv.versions[rel.ID], false
}

// Version returns the current schema version for a relation, or 0 if unknown.
func (sv *SchemaVersion) Version(relationID uint32) uint64 {
	sv.mu.RLock()
	defer sv.mu.RUnlock()
	return sv.versions[relationID]
}

// Reset clears all tracked versions.
func (sv *SchemaVersion) Reset() {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	sv.versions = make(map[uint32]uint64)
	sv.fingerprints = make(map[uint32]string)
}

// fingerprint produces a stable string representation of a relation's columns.
func fingerprint(rel Relation) string {
	s := fmt.Sprintf("%s.%s:", rel.Namespace, rel.Name)
	for _, c := range rel.Columns {
		s += fmt.Sprintf("%s(%d)", c.Name, c.DataType)
	}
	return s
}
