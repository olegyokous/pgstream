package wal

import (
	"fmt"
	"time"
)

// EnvelopeFormat controls how messages are wrapped.
type EnvelopeFormat string

const (
	EnvelopeFormatNone    EnvelopeFormat = "none"
	EnvelopeFormatDefault EnvelopeFormat = "default"
)

// Envelope wraps a formatted WAL message with metadata for downstream consumers.
type Envelope struct {
	ID        string         `json:"id"`
	Timestamp time.Time      `json:"timestamp"`
	Source    string         `json:"source"`
	Payload   string         `json:"payload"`
	Meta      map[string]any `json:"meta,omitempty"`
}

// String returns a human-readable representation of the envelope.
func (e Envelope) String() string {
	return fmt.Sprintf("[%s] %s @ %s: %s", e.ID, e.Source, e.Timestamp.Format(time.RFC3339), e.Payload)
}

// Enveloper wraps formatted message bytes into an Envelope.
type Enveloper struct {
	source string
	clock  func() time.Time
	nextID func() string
}

// EnveloperOption configures an Enveloper.
type EnveloperOption func(*Enveloper)

// WithEnvelopeSource sets the source field on all produced envelopes.
func WithEnvelopeSource(source string) EnveloperOption {
	return func(e *Enveloper) { e.source = source }
}

// withEnvelopeClock overrides the clock (for testing).
func withEnvelopeClock(fn func() time.Time) EnveloperOption {
	return func(e *Enveloper) { e.clock = fn }
}

// withEnvelopeIDGen overrides the ID generator (for testing).
func withEnvelopeIDGen(fn func() string) EnveloperOption {
	return func(e *Enveloper) { e.nextID = fn }
}

// NewEnveloper creates an Enveloper with the given options.
func NewEnveloper(opts ...EnveloperOption) *Enveloper {
	env := &Enveloper{
		source: "pgstream",
		clock:  time.Now,
		nextID: newULID,
	}
	for _, o := range opts {
		o(env)
	}
	return env
}

// Wrap encodes a WAL message payload into an Envelope.
func (e *Enveloper) Wrap(payload string, meta map[string]any) Envelope {
	return Envelope{
		ID:        e.nextID(),
		Timestamp: e.clock(),
		Source:    e.source,
		Payload:   payload,
		Meta:      meta,
	}
}
