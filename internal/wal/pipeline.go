package wal

import (
	"context"
	"fmt"
	"io"
)

// Pipeline wires together a Listener, Decoder, Filter, Transformer, and
// Formatter, writing formatted output to a writer.
type Pipeline struct {
	listener    *Listener
	decoder     *Decoder
	filter      *Filter
	transformer *Transformer
	formatter   Formatter
	out         io.Writer
	checkpoint  *CheckpointManager
}

// PipelineOption is a functional option for Pipeline.
type PipelineOption func(*Pipeline)

// WithCheckpoint attaches a CheckpointManager to the pipeline so that
// successfully processed LSNs are acknowledged back to Postgres.
func WithCheckpoint(cm *CheckpointManager) PipelineOption {
	return func(p *Pipeline) { p.checkpoint = cm }
}

// WithTransformer attaches a Transformer to the pipeline.
func WithTransformer(tr *Transformer) PipelineOption {
	return func(p *Pipeline) { p.transformer = tr }
}

// NewPipeline constructs a Pipeline from its required components and any
// optional overrides supplied via PipelineOption.
func NewPipeline(
	l *Listener,
	d *Decoder,
	f *Filter,
	fmt Formatter,
	out io.Writer,
	opts ...PipelineOption,
) *Pipeline {
	p := &Pipeline{
		listener:  l,
		decoder:   d,
		filter:    f,
		formatter: fmt,
		out:       out,
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Run starts the pipeline. It blocks until ctx is cancelled or a fatal error
// occurs. Filtered-out or nil-transformed messages are silently skipped.
func (p *Pipeline) Run(ctx context.Context) error {
	return p.listener.Listen(ctx, func(raw []byte, lsn uint64) error {
		msg, err := p.decoder.Decode(raw)
		if err != nil || msg == nil {
			return nil // skip non-data messages
		}

		if !p.filter.Match(msg) {
			return nil
		}

		if p.transformer != nil {
			msg = p.transformer.Apply(msg)
			if msg == nil {
				return nil
			}
		}

		line, err := p.formatter.Format(msg)
		if err != nil {
			return fmt.Errorf("format: %w", err)
		}

		if _, err := fmt.Fprintln(p.out, line); err != nil {
			return fmt.Errorf("write: %w", err)
		}

		if p.checkpoint != nil {
			p.checkpoint.Track(lsn)
		}
		return nil
	})
}
