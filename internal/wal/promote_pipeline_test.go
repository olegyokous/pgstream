package wal

import (
	"testing"
)

func TestPromoter_IntegratesWithLabeler(t *testing.T) {
	promoter, err := NewPromoter([]PromoterRule{
		{Table: "events", Column: "event_type", MetaKey: "type"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	labeler, err := NewLabeler(
		map[string]string{"source": "wal"},
		WithLabelOverwrite(false),
	)
	if err != nil {
		t.Fatalf("unexpected labeler error: %v", err)
	}

	msg := promoteMsg("events", "INSERT", []Column{
		{Name: "event_type", Value: "click"},
		{Name: "user_id", Value: "5"},
	})

	out := promoter.Apply(msg)
	out = labeler.Apply(out)

	if out.Meta["type"] != "click" {
		t.Errorf("expected type=click, got %q", out.Meta["type"])
	}
	if out.Meta["source"] != "wal" {
		t.Errorf("expected source=wal, got %q", out.Meta["source"])
	}
}

func TestPromoter_ChainedWithTagger(t *testing.T) {
	promoter, _ := NewPromoter([]PromoterRule{
		{Column: "tenant_id", MetaKey: "tenant"},
	})
	tagger, _ := NewTagger(
		map[string]string{"env": "prod"},
		nil,
	)

	msg := promoteMsg("accounts", "UPDATE", []Column{
		{Name: "tenant_id", Value: "acme"},
	})

	out := promoter.Apply(msg)
	out = tagger.Apply(out)

	if out.Meta["tenant"] != "acme" {
		t.Errorf("expected tenant=acme, got %q", out.Meta["tenant"])
	}
	if out.Meta["env"] != "prod" {
		t.Errorf("expected env=prod, got %q", out.Meta["env"])
	}
}

func TestPromoter_MultipleRulesApplied(t *testing.T) {
	promoter, _ := NewPromoter([]PromoterRule{
		{Column: "request_id", MetaKey: "rid"},
		{Column: "session_id", MetaKey: "sid"},
	})

	msg := promoteMsg("logs", "INSERT", []Column{
		{Name: "request_id", Value: "req-1"},
		{Name: "session_id", Value: "sess-9"},
	})

	out := promoter.Apply(msg)

	if out.Meta["rid"] != "req-1" {
		t.Errorf("expected rid=req-1, got %q", out.Meta["rid"])
	}
	if out.Meta["sid"] != "sess-9" {
		t.Errorf("expected sid=sess-9, got %q", out.Meta["sid"])
	}
}
