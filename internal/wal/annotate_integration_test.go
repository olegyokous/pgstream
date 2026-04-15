package wal

import (
	"sync"
	"testing"
)

func TestAnnotator_ConcurrentApplyIsSafe(t *testing.T) {
	a, err := NewAnnotator([]AnnotateRule{
		{Key: "env", Value: "prod"},
		{Table: "users", Key: "pii", Value: "true"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := annotateMsg("users", "INSERT")
			out := a.Apply(msg)
			if out.Meta["env"] != "prod" {
				t.Errorf("concurrent apply: expected env=prod")
			}
		}()
	}
	wg.Wait()
}

func TestAnnotator_MultipleTablesSelective(t *testing.T) {
	a, err := NewAnnotator([]AnnotateRule{
		{Table: "users", Key: "pii", Value: "true"},
		{Table: "orders", Key: "financial", Value: "true"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tests := []struct {
		table string
		key   string
	}{
		{"users", "pii"},
		{"orders", "financial"},
	}
	for _, tc := range tests {
		msg := annotateMsg(tc.table, "INSERT")
		out := a.Apply(msg)
		if out.Meta[tc.key] != "true" {
			t.Errorf("table %s: expected %s=true, got %v", tc.table, tc.key, out.Meta)
		}
	}

	// unrelated table gets no annotations
	msg := annotateMsg("products", "INSERT")
	out := a.Apply(msg)
	if len(out.Meta) != 0 {
		t.Errorf("unrelated table should have no annotations, got %v", out.Meta)
	}
}
