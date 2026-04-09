package wal

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
)

// Decoder decodes pglogrepl logical replication messages into WAL Messages.
type Decoder struct {
	relations map[uint32]*Relation
}

// NewDecoder creates a new Decoder instance.
func NewDecoder() *Decoder {
	return &Decoder{
		relations: make(map[uint32]*Relation),
	}
}

// Decode converts a pglogrepl logical message into a *Message.
// Returns nil, nil for non-DML messages (e.g. Relation, Begin, Commit).
func (d *Decoder) Decode(lsn pglogrepl.LSN, msg pglogrepl.Message) (*Message, error) {
	switch v := msg.(type) {
	case *pglogrepl.RelationMessage:
		d.storeRelation(v)
		return nil, nil
	case *pglogrepl.InsertMessage:
		return d.decodeInsert(lsn, v)
	case *pglogrepl.UpdateMessage:
		return d.decodeUpdate(lsn, v)
	case *pglogrepl.DeleteMessage:
		return d.decodeDelete(lsn, v)
	case *pglogrepl.TruncateMessage:
		return d.decodeTruncate(lsn, v)
	}
	return nil, nil
}

func (d *Decoder) storeRelation(v *pglogrepl.RelationMessage) {
	rel := &Relation{ID: v.RelationID, Schema: v.Namespace, Table: v.RelationName}
	for _, col := range v.Columns {
		rel.Columns = append(rel.Columns, RelationColumn{Name: col.Name, Type: col.DataType})
	}
	d.relations[v.RelationID] = rel
}

func (d *Decoder) decodeInsert(lsn pglogrepl.LSN, v *pglogrepl.InsertMessage) (*Message, error) {
	rel, err := d.getRelation(v.RelationID)
	if err != nil {
		return nil, err
	}
	cols := tupleToColumns(v.Tuple, rel)
	return &Message{LSN: lsn.String(), Timestamp: time.Now().UTC(), Action: ActionInsert,
		Schema: rel.Schema, Table: rel.Table, Columns: cols}, nil
}

func (d *Decoder) decodeUpdate(lsn pglogrepl.LSN, v *pglogrepl.UpdateMessage) (*Message, error) {
	rel, err := d.getRelation(v.RelationID)
	if err != nil {
		return nil, err
	}
	cols := tupleToColumns(v.NewTuple, rel)
	var oldKeys []Column
	if v.OldTuple != nil {
		oldKeys = tupleToColumns(v.OldTuple, rel)
	}
	return &Message{LSN: lsn.String(), Timestamp: time.Now().UTC(), Action: ActionUpdate,
		Schema: rel.Schema, Table: rel.Table, Columns: cols, OldKeys: oldKeys}, nil
}

func (d *Decoder) decodeDelete(lsn pglogrepl.LSN, v *pglogrepl.DeleteMessage) (*Message, error) {
	rel, err := d.getRelation(v.RelationID)
	if err != nil {
		return nil, err
	}
	var oldKeys []Column
	if v.OldTuple != nil {
		oldKeys = tupleToColumns(v.OldTuple, rel)
	}
	return &Message{LSN: lsn.String(), Timestamp: time.Now().UTC(), Action: ActionDelete,
		Schema: rel.Schema, Table: rel.Table, OldKeys: oldKeys}, nil
}

func (d *Decoder) decodeTruncate(lsn pglogrepl.LSN, v *pglogrepl.TruncateMessage) (*Message, error) {
	return &Message{LSN: lsn.String(), Timestamp: time.Now().UTC(), Action: ActionTruncate}, nil
}

func (d *Decoder) getRelation(id uint32) (*Relation, error) {
	rel, ok := d.relations[id]
	if !ok {
		return nil, fmt.Errorf("unknown relation id %d", id)
	}
	return rel, nil
}

func tupleToColumns(tuple *pglogrepl.TupleData, rel *Relation) []Column {
	if tuple == nil {
		return nil
	}
	cols := make([]Column, 0, len(tuple.Columns))
	for i, tc := range tuple.Columns {
		name := ""
		if i < len(rel.Columns) {
			name = rel.Columns[i].Name
		}
		var val any
		switch tc.DataType {
		case 'n':
			val = nil
		case 't':
			val = string(tc.Data)
		}
		cols = append(cols, Column{Name: name, Value: val})
	}
	return cols
}
