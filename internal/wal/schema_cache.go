package wal

import (
	"fmt"
	"sync"
)

// ColumnInfo holds metadata about a single table column.
type ColumnInfo struct {
	Name     string
	TypeOID  uint32
	Nullable bool
}

// RelationInfo holds metadata about a PostgreSQL relation (table).
type RelationInfo struct {
	Schema  string
	Table   string
	Columns []ColumnInfo
}

// SchemaCache stores relation metadata keyed by relation ID.
type SchemaCache struct {
	mu      sync.RWMutex
	relations map[uint32]*RelationInfo
}

// NewSchemaCache creates an empty SchemaCache.
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		relations: make(map[uint32]*RelationInfo),
	}
}

// Store saves or updates a relation in the cache.
func (c *SchemaCache) Store(id uint32, info *RelationInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.relations[id] = info
}

// Lookup retrieves a relation by ID. Returns an error if not found.
func (c *SchemaCache) Lookup(id uint32) (*RelationInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	info, ok := c.relations[id]
	if !ok {
		return nil, fmt.Errorf("schema cache: unknown relation id %d", id)
	}
	return info, nil
}

// Delete removes a relation from the cache.
func (c *SchemaCache) Delete(id uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.relations, id)
}

// Size returns the number of cached relations.
func (c *SchemaCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.relations)
}
