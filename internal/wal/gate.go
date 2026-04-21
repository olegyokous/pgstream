package wal

import "sync/atomic"

// Gate is a binary on/off switch that controls whether messages are allowed
// through. When closed, Apply returns nil (dropping the message). When open,
// the message passes through unchanged.
type Gate struct {
	open atomic.Bool
}

// NewGate creates a Gate. If startOpen is true the gate begins in the open
// (pass-through) state; otherwise it starts closed (dropping).
func NewGate(startOpen bool) *Gate {
	g := &Gate{}
	if startOpen {
		g.open.Store(true)
	}
	return g
}

// Open allows messages to pass through.
func (g *Gate) Open() { g.open.Store(true) }

// Close causes messages to be dropped.
func (g *Gate) Close() { g.open.Store(false) }

// IsOpen reports whether the gate is currently open.
func (g *Gate) IsOpen() bool { return g.open.Load() }

// Apply returns msg unchanged when the gate is open, or nil when closed.
func (g *Gate) Apply(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	if !g.open.Load() {
		return nil
	}
	return msg
}
