package wal

import (
	"sync"
	"testing"
)

func sampleRelation() *RelationInfo {
	return &RelationInfo{
		Schema: "public",
		Table:  "users",
		Columns: []ColumnInfo{
			{Name: "id", TypeOID: 23, Nullable: false},
			{Name: "email", TypeOID: 25, Nullable: true},
		},
	}
}

func TestSchemaCache_StoreAndLookup(t *testing.T) {
	c := NewSchemaCache()
	c.Store(1, sampleRelation())

	info, err := c.Lookup(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Table != "users" {
		t.Errorf("expected table 'users', got %q", info.Table)
	}
}

func TestSchemaCache_UnknownRelation(t *testing.T) {
	c := NewSchemaCache()
	_, err := c.Lookup(99)
	if err == nil {
		t.Fatal("expected error for unknown relation, got nil")
	}
}

func TestSchemaCache_Delete(t *testing.T) {
	c := NewSchemaCache()
	c.Store(2, sampleRelation())
	c.Delete(2)

	if c.Size() != 0 {
		t.Errorf("expected size 0 after delete, got %d", c.Size())
	}
	_, err := c.Lookup(2)
	if err == nil {
		t.Fatal("expected error after delete, got nil")
	}
}

func TestSchemaCache_Size(t *testing.T) {
	c := NewSchemaCache()
	if c.Size() != 0 {
		t.Errorf("expected initial size 0, got %d", c.Size())
	}
	c.Store(1, sampleRelation())
	c.Store(2, sampleRelation())
	if c.Size() != 2 {
		t.Errorf("expected size 2, got %d", c.Size())
	}
}

func TestSchemaCache_ConcurrentAccess(t *testing.T) {
	c := NewSchemaCache()
	var wg sync.WaitGroup
	for i := uint32(0); i < 20; i++ {
		wg.Add(1)
		go func(id uint32) {
			defer wg.Done()
			c.Store(id, sampleRelation())
			_, _ = c.Lookup(id)
		}(i)
	}
	wg.Wait()
	if c.Size() != 20 {
		t.Errorf("expected 20 entries after concurrent stores, got %d", c.Size())
	}
}
