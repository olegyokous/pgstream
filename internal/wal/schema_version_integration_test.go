package wal_test

import (
	"sync"
	"testing"

	"github.com/your-org/pgstream/internal/wal"
)

func TestSchemaVersion_ConcurrentObserveIsSafe(t *testing.T) {
	sv := wal.NewSchemaVersion()
	rel := wal.Relation{
		ID:        42,
		Namespace: "public",
		Name:      "orders",
		Columns:   []wal.Column{{Name: "id", DataType: 23}},
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sv.Observe(rel)
			sv.Version(42)
		}()
	}
	wg.Wait()

	v := sv.Version(42)
	if v == 0 {
		t.Fatal("expected non-zero version after concurrent observes")
	}
}

func TestSchemaVersion_DetectsSchemaEvolution(t *testing.T) {
	sv := wal.NewSchemaVersion()

	schemas := []wal.Relation{
		{ID: 1, Namespace: "public", Name: "tbl", Columns: []wal.Column{{Name: "id", DataType: 23}}},
		{ID: 1, Namespace: "public", Name: "tbl", Columns: []wal.Column{{Name: "id", DataType: 23}, {Name: "ts", DataType: 1114}}},
		{ID: 1, Namespace: "public", Name: "tbl", Columns: []wal.Column{{Name: "id", DataType: 23}, {Name: "ts", DataType: 1114}, {Name: "val", DataType: 701}}},
	}

	for i, rel := range schemas {
		v, changed := sv.Observe(rel)
		if !changed {
			t.Fatalf("step %d: expected changed=true", i)
		}
		if v != uint64(i+1) {
			t.Fatalf("step %d: expected version %d, got %d", i, i+1, v)
		}
	}
}

// TestSchemaVersion_ReobservingSameSchemaDoesNotIncrement verifies that
// observing an identical relation twice does not bump the version.
func TestSchemaVersion_ReobservingSameSchemaDoesNotIncrement(t *testing.T) {
	sv := wal.NewSchemaVersion()
	rel := wal.Relation{
		ID:        7,
		Namespace: "public",
		Name:      "users",
		Columns:   []wal.Column{{Name: "id", DataType: 23}, {Name: "email", DataType: 25}},
	}

	v1, changed1 := sv.Observe(rel)
	if !changed1 {
		t.Fatal("first observe: expected changed=true")
	}

	v2, changed2 := sv.Observe(rel)
	if changed2 {
		t.Fatal("second observe of identical schema: expected changed=false")
	}
	if v2 != v1 {
		t.Fatalf("expected version to remain %d after re-observe, got %d", v1, v2)
	}
}
