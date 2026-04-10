package wal

import (
	"context"
	"fmt"
	"io"
)

// Pipeline wires together decoding, filtering, formatting, and writing of WAL
// messages. An optional CheckpointManager can be supplied so that each
// successfully written message advances the replication cursor.
type Pipeline struct {
	decoder    *Decoder
	filter     *Filter
	formatter  Formatter
	writer     io.Writer
	checkpoint *CheckpointManager
}

// PipelineOption is a functional option for Pipeline.
type PipelineOption func(*Pipeline)

// WithCheckpoint attaches a CheckpointManager to the pipeline.
func WithCheckpoint(cm *CheckpointManager) PipelineOption {
	return func(p *Pipeline) {
		p.checkpoint = cm
	}
}

// NewPipeline constructs a Pipeline from its components.
func NewPipeline(dec *Decoder, f *Filter, fmt Formatter, w io.Writer, opts ...PipelineOption) *Pipeline {
	p := &Pipeline{
		decoder:   dec,
		filter:    f,
		formatter: fmt,
		writer:    w,
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Process decodes raw WAL data, applies the filter, formats the result, and
// writes it to the configured writer. lsn is the log-sequence number of the
// message; it is forwarded to the CheckpointManager when present.
func (p *Pipeline) Process(ctx context.Context, data []byte, lsn uint64) error {
	msg, err := p.decoder.Decode(data)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if msg == nil {
		return nil
	}

	if !p.filter.Match(msg) {
		return nil
	}

	out, err := p.formatter.Format(msg)
	if err != nil {
		return fmt.Errorf("format: %w", err)
	}

	if _, err := fmt.Fprintln(p.writer, string(out)); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	if p.checkpoint != nil {
		p.checkpoint.Track(lsn)
	}
	return nil
}
