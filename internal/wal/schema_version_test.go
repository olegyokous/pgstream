package wal

import (
	"testing"
)

func schemRel(id uint32, cols ...Column) Relation {
	return Relation{
		ID:        id,
		Namespace: "public",
		Name:      "events",
		Columns:   cols,
	}
}

func TestSchemaVersion_InitialObserveIsChanged(t *testing.T) {
	sv := NewSchemaVersion()
	rel := schemRel(1, Column{Name: "id", DataType: 23})

	v, changed := sv.Observe(rel)
	if !changed {
		t.Fatal("expected changed=true on first observe")
	}
	if v != 1 {
		t.Fatalf("expected version 1, got %d", v)
	}
}

func TestSchemaVersion_SameSchemaNotChanged(t *testing.T) {
	sv := NewSchemaVersion()
	rel := schemRel(1, Column{Name: "id", DataType: 23})

	sv.Observe(rel)
	v, changed := sv.Observe(rel)
	if changed {
		t.Fatal("expected changed=false on identical re-observe")
	}
	if v != 1 {
		t.Fatalf("expected version 1, got %d", v)
	}
}

func TestSchemaVersion_ColumnAddedIsChanged(t *testing.T) {
	sv := NewSchemaVersion()
	rel1 := schemRel(1, Column{Name: "id", DataType: 23})
	rel2 := schemRel(1, Column{Name: "id", DataType: 23}, Column{Name: "name", DataType: 25})

	sv.Observe(rel1)
	v, changed := sv.Observe(rel2)
	if !changed {
		t.Fatal("expected changed=true after column added")
	}
	if v != 2 {
		t.Fatalf("expected version 2, got %d", v)
	}
}

func TestSchemaVersion_VersionReturnsZeroForUnknown(t *testing.T) {
	sv := NewSchemaVersion()
	if v := sv.Version(999); v != 0 {
		t.Fatalf("expected 0 for unknown relation, got %d", v)
	}
}

func TestSchemaVersion_ResetClearsState(t *testing.T) {
	sv := NewSchemaVersion()
	rel := schemRel(1, Column{Name: "id", DataType: 23})
	sv.Observe(rel)

	sv.Reset()

	if v := sv.Version(1); v != 0 {
		t.Fatalf("expected 0 after reset, got %d", v)
	}
	_, changed := sv.Observe(rel)
	if !changed {
		t.Fatal("expected changed=true after reset")
	}
}

func TestSchemaVersion_MultipleRelationsAreIndependent(t *testing.T) {
	sv := NewSchemaVersion()
	relA := Relation{ID: 1, Namespace: "public", Name: "a", Columns: []Column{{Name: "x", DataType: 23}}}
	relB := Relation{ID: 2, Namespace: "public", Name: "b", Columns: []Column{{Name: "y", DataType: 25}}}

	sv.Observe(relA)
	sv.Observe(relB)

	if sv.Version(1) != 1 || sv.Version(2) != 1 {
		t.Fatal("expected each relation to be at version 1")
	}

	relAChanged := Relation{ID: 1, Namespace: "public", Name: "a", Columns: []Column{{Name: "x", DataType: 23}, {Name: "z", DataType: 16}}}
	sv.Observe(relAChanged)

	if sv.Version(1) != 2 {
		t.Fatalf("expected relA at version 2, got %d", sv.Version(1))
	}
	if sv.Version(2) != 1 {
		t.Fatalf("expected relB still at version 1, got %d", sv.Version(2))
	}
}
