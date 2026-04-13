package wal_test

import (
	"testing"

	"github.com/your-org/pgstream/internal/wal"
)

// TestSchemaVersion_TracksThroughDecoder verifies that SchemaVersion integrates
// naturally with Relation data produced by the decoder layer.
func TestSchemaVersion_TracksThroughDecoder(t *testing.T) {
	sv := wal.NewSchemaVersion()

	rel := wal.Relation{
		ID:        7,
		Namespace: "app",
		Name:      "users",
		Columns: []wal.Column{
			{Name: "id", DataType: 23},
			{Name: "email", DataType: 25},
		},
	}

	v1, changed := sv.Observe(rel)
	if !changed || v1 != 1 {
		t.Fatalf("expected v=1 changed=true, got v=%d changed=%v", v1, changed)
	}

	// Simulate same relation arriving again (no DDL change)
	v2, changed := sv.Observe(rel)
	if changed || v2 != 1 {
		t.Fatalf("expected v=1 changed=false, got v=%d changed=%v", v2, changed)
	}

	// DDL: column type changed
	relV2 := wal.Relation{
		ID:        7,
		Namespace: "app",
		Name:      "users",
		Columns: []wal.Column{
			{Name: "id", DataType: 23},
			{Name: "email", DataType: 25},
			{Name: "created_at", DataType: 1114},
		},
	}

	v3, changed := sv.Observe(relV2)
	if !changed || v3 != 2 {
		t.Fatalf("expected v=2 changed=true after DDL, got v=%d changed=%v", v3, changed)
	}
}

func TestSchemaVersion_ResetAndReobserveStartsFresh(t *testing.T) {
	sv := wal.NewSchemaVersion()
	rel := wal.Relation{
		ID:        3,
		Namespace: "public",
		Name:      "items",
		Columns:   []wal.Column{{Name: "sku", DataType: 25}},
	}

	sv.Observe(rel)
	sv.Observe(rel)
	sv.Reset()

	v, changed := sv.Observe(rel)
	if !changed {
		t.Fatal("expected changed=true after reset and re-observe")
	}
	if v != 1 {
		t.Fatalf("expected version 1 after reset, got %d", v)
	}
}
