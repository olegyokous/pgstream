package wal

import (
	"testing"
)

// TestEnricher_IntegratesWithTransformer verifies that an Enricher and a
// Transformer can be chained: the Enricher stamps metadata and the Transformer
// masks a column in the same pass.
func TestEnricher_IntegratesWithTransformer(t *testing.T) {
	enricher, err := NewEnricher([]EnrichRule{{Key: "source", Value: "cdc"}}, nil)
	if err != nil {
		t.Fatalf("NewEnricher: %v", err)
	}
	transformer := NewTransformer(MaskColumns("password"))

	msg := &Message{
		Table:  "users",
		Action: "INSERT",
		Columns: []Column{
			{Name: "email", Value: "alice@example.com"},
			{Name: "password", Value: "secret"},
		},
	}

	msg = enricher.Apply(msg)
	msg = transformer.Apply(msg)

	if msg.Meta["source"] != "cdc" {
		t.Errorf("expected meta source=cdc, got %q", msg.Meta["source"])
	}
	for _, col := range msg.Columns {
		if col.Name == "password" && col.Value != "***" {
			t.Errorf("expected password masked, got %q", col.Value)
		}
	}
}

// TestEnricher_TableScopedDoesNotPollute ensures that enrichment scoped to one
// table does not bleed into messages from another table in the same stream.
func TestEnricher_TableScopedDoesNotPollute(t *testing.T) {
	e, _ := NewEnricher([]EnrichRule{{Key: "pii", Value: "true"}}, []string{"users"})

	msgs := []*Message{
		{Table: "users", Action: "INSERT"},
		{Table: "orders", Action: "INSERT"},
		{Table: "users", Action: "UPDATE"},
	}

	for _, m := range msgs {
		e.Apply(m)
	}

	if msgs[0].Meta["pii"] != "true" {
		t.Errorf("users INSERT: expected pii=true")
	}
	if msgs[1].Meta != nil && msgs[1].Meta["pii"] != "" {
		t.Errorf("orders INSERT: unexpected pii tag")
	}
	if msgs[2].Meta["pii"] != "true" {
		t.Errorf("users UPDATE: expected pii=true")
	}
}
