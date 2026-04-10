package wal

import "testing"

// TestSchemaCache_OverwriteRelation ensures that storing a relation with an
// existing ID replaces the previous entry rather than appending.
func TestSchemaCache_OverwriteRelation(t *testing.T) {
	c := NewSchemaCache()

	first := &RelationInfo{Schema: "public", Table: "orders", Columns: nil}
	second := &RelationInfo{Schema: "public", Table: "orders_v2", Columns: nil}

	c.Store(10, first)
	c.Store(10, second)

	if c.Size() != 1 {
		t.Errorf("expected size 1 after overwrite, got %d", c.Size())
	}

	info, err := c.Lookup(10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Table != "orders_v2" {
		t.Errorf("expected overwritten table 'orders_v2', got %q", info.Table)
	}
}

// TestSchemaCache_MultipleRelations verifies independent storage of several
// relations and that lookups return the correct entry for each.
func TestSchemaCache_MultipleRelations(t *testing.T) {
	c := NewSchemaCache()

	relations := map[uint32]string{
		1: "users",
		2: "products",
		3: "orders",
	}

	for id, table := range relations {
		c.Store(id, &RelationInfo{Schema: "public", Table: table})
	}

	for id, expected := range relations {
		info, err := c.Lookup(id)
		if err != nil {
			t.Errorf("lookup(%d): unexpected error: %v", id, err)
			continue
		}
		if info.Table != expected {
			t.Errorf("lookup(%d): expected %q, got %q", id, expected, info.Table)
		}
	}
}
