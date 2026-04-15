package wal

import (
	"testing"
)

func promoteMsg(table, action string, cols []Column) *Message {
	return &Message{Table: table, Action: Action(action), Columns: cols}
}

func TestNewPromoter_NoRulesErrors(t *testing.T) {
	_, err := NewPromoter(nil)
	if err == nil {
		t.Fatal("expected error for empty rules")
	}
}

func TestNewPromoter_EmptyColumnErrors(t *testing.T) {
	_, err := NewPromoter([]PromoterRule{{Column: "", MetaKey: "k"}})
	if err == nil {
		t.Fatal("expected error for empty column")
	}
}

func TestNewPromoter_EmptyMetaKeyErrors(t *testing.T) {
	_, err := NewPromoter([]PromoterRule{{Column: "id", MetaKey: ""}})
	if err == nil {
		t.Fatal("expected error for empty meta_key")
	}
}

func TestPromoter_NilMessagePassthrough(t *testing.T) {
	p, _ := NewPromoter([]PromoterRule{{Column: "id", MetaKey: "row_id"}})
	if got := p.Apply(nil); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestPromoter_PromotesColumnToMeta(t *testing.T) {
	p, _ := NewPromoter([]PromoterRule{{Column: "user_id", MetaKey: "uid"}})
	msg := promoteMsg("users", "INSERT", []Column{
		{Name: "user_id", Value: "42"},
		{Name: "email", Value: "a@b.com"},
	})
	out := p.Apply(msg)
	if out.Meta["uid"] != "42" {
		t.Fatalf("expected uid=42, got %q", out.Meta["uid"])
	}
}

func TestPromoter_NonMatchingTableSkipped(t *testing.T) {
	p, _ := NewPromoter([]PromoterRule{{Table: "orders", Column: "order_id", MetaKey: "oid"}})
	msg := promoteMsg("users", "INSERT", []Column{
		{Name: "order_id", Value: "99"},
	})
	out := p.Apply(msg)
	if out.Meta != nil && out.Meta["oid"] != "" {
		t.Fatal("expected no promotion for non-matching table")
	}
}

func TestPromoter_NonMatchingActionSkipped(t *testing.T) {
	p, _ := NewPromoter([]PromoterRule{{Action: "DELETE", Column: "id", MetaKey: "del_id"}})
	msg := promoteMsg("users", "INSERT", []Column{
		{Name: "id", Value: "7"},
	})
	out := p.Apply(msg)
	if out.Meta != nil && out.Meta["del_id"] != "" {
		t.Fatal("expected no promotion for non-matching action")
	}
}

func TestPromoter_NilColumnValueSkipped(t *testing.T) {
	p, _ := NewPromoter([]PromoterRule{{Column: "id", MetaKey: "row_id"}})
	msg := promoteMsg("users", "INSERT", []Column{
		{Name: "id", Value: nil},
	})
	out := p.Apply(msg)
	if out.Meta != nil && out.Meta["row_id"] != "" {
		t.Fatal("expected no promotion for nil column value")
	}
}
