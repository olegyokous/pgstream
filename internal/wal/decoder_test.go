package wal

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestDecoder_StoresRelationAndDecodesInsert(t *testing.T) {
	d := NewDecoder()

	relMsg := &pglogrepl.RelationMessage{
		RelationID:   1,
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id"},
			{Name: "name"},
		},
	}
	msg, err := d.Decode(0, relMsg)
	if err != nil {
		t.Fatalf("unexpected error storing relation: %v", err)
	}
	if msg != nil {
		t.Fatalf("expected nil message for relation, got %+v", msg)
	}

	insertMsg := &pglogrepl.InsertMessage{
		RelationID: 1,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("42")},
				{DataType: 't', Data: []byte("alice")},
			},
		},
	}
	msg, err = d.Decode(100, insertMsg)
	if err != nil {
		t.Fatalf("unexpected error decoding insert: %v", err)
	}
	if msg == nil {
		t.Fatal("expected non-nil message")
	}
	if msg.Action != ActionInsert {
		t.Errorf("expected INSERT, got %s", msg.Action)
	}
	if msg.Schema != "public" || msg.Table != "users" {
		t.Errorf("unexpected schema/table: %s.%s", msg.Schema, msg.Table)
	}
	if len(msg.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(msg.Columns))
	}
	if msg.Columns[0].Name != "id" || msg.Columns[0].Value != "42" {
		t.Errorf("unexpected column[0]: %+v", msg.Columns[0])
	}
}

func TestDecoder_UnknownRelation(t *testing.T) {
	d := NewDecoder()
	insertMsg := &pglogrepl.InsertMessage{
		RelationID: 999,
		Tuple:      &pglogrepl.TupleData{},
	}
	_, err := d.Decode(0, insertMsg)
	if err == nil {
		t.Fatal("expected error for unknown relation")
	}
}

func TestDecoder_NullColumn(t *testing.T) {
	d := NewDecoder()
	d.storeRelation(&pglogrepl.RelationMessage{
		RelationID: 2, Namespace: "public", RelationName: "orders",
		Columns: []*pglogrepl.RelationMessageColumn{{Name: "id"}},
	})
	insertMsg := &pglogrepl.InsertMessage{
		RelationID: 2,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{{DataType: 'n'}},
		},
	}
	msg, err := d.Decode(0, insertMsg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Columns[0].Value != nil {
		t.Errorf("expected nil value for null column")
	}
}
