package wal

import "fmt"

// Evolver watches for schema changes on a message and invokes a callback
// when the schema fingerprint changes relative to the last observed version.

type EvolveCallback func(table string, version int, msg *Message) error

type Evolver struct {
	sv       *SchemaVersion
	callback EvolveCallback
	table    string
}

type EvolverOption func(*Evolver)

func WithEvolverTable(table string) EvolverOption {
	return func(e *Evolver) { e.table = table }
}

func NewEvolver(cb EvolveCallback, opts ...EvolverOption) (*Evolver, error) {
	if cb == nil {
		return nil, fmt.Errorf("evolver: callback must not be nil")
	}
	e := &Evolver{
		sv:       NewSchemaVersion(),
		callback: cb,
	}
	for _, o := range opts {
		o(e)
	}
	return e, nil
}

// Apply checks whether the message's relation schema has changed and fires the
// callback when it has. The original message is always returned unchanged.
func (e *Evolver) Apply(msg *Message) (*Message, error) {
	if msg == nil {
		return nil, nil
	}
	if e.table != "" && msg.Table != e.table {
		return msg, nil
	}
	changed, err := e.sv.Observe(msg.Relation)
	if err != nil {
		return msg, fmt.Errorf("evolver: observe: %w", err)
	}
	if changed {
		v := e.sv.Version(msg.Table)
		if err := e.callback(msg.Table, v, msg); err != nil {
			return msg, fmt.Errorf("evolver: callback: %w", err)
		}
	}
	return msg, nil
}
