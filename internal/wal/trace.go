package wal

import (
	"fmt"
	"io"
	"os"
	"time"
)

// Tracer attaches a trace ID and hop count to each message as it passes
// through the pipeline, writing a human-readable trace line to a writer.
type Tracer struct {
	writer  io.Writer
	field   string
	hopField string
	table   string // empty means all tables
}

// WithTracerTable scopes the tracer to a single table.
func WithTracerTable(table string) func(*Tracer) {
	return func(t *Tracer) { t.table = table }
}

// WithTracerField sets the meta key used to store the trace ID.
func WithTracerField(field string) func(*Tracer) {
	return func(t *Tracer) { t.field = field }
}

// WithTracerWriter sets the writer for trace output (default: os.Stderr).
func WithTracerWriter(w io.Writer) func(*Tracer) {
	return func(t *Tracer) { t.writer = w }
}

// NewTracer returns a Tracer that stamps trace metadata onto messages.
func NewTracer(opts ...func(*Tracer)) *Tracer {
	t := &Tracer{
		writer:   os.Stderr,
		field:    "trace_id",
		hopField: "trace_hop",
	}
	for _, o := range opts {
		o(t)
	}
	return t
}

// Apply stamps trace metadata onto msg and emits a trace line.
// Returns the (possibly modified) message and nil, or nil and an error.
func (t *Tracer) Apply(msg *Message) (*Message, error) {
	if msg == nil {
		return nil, nil
	}
	if t.table != "" && msg.Table != t.table {
		return msg, nil
	}
	if msg.Meta == nil {
		msg.Meta = make(map[string]string)
	}
	traceID, ok := msg.Meta[t.field]
	if !ok || traceID == "" {
		traceID = newULID()
		msg.Meta[t.field] = traceID
	}
	hop := 0
	if v, exists := msg.Meta[t.hopField]; exists {
		fmt.Sscanf(v, "%d", &hop)
	}
	hop++
	msg.Meta[t.hopField] = fmt.Sprintf("%d", hop)

	fmt.Fprintf(t.writer, "[trace] %s id=%s hop=%d table=%s action=%s lsn=%d\n",
		time.Now().UTC().Format(time.RFC3339Nano),
		traceID, hop, msg.Table, msg.Action, msg.LSN)
	return msg, nil
}
