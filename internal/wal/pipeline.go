package wal

import (
	"context"
	"fmt"
	"io"
)

// Pipeline wires together a filter and formatter to process WAL messages.
type Pipeline struct {
	filter    *Filter
	formatter Formatter
	writer    io.Writer
}

// Formatter is the interface implemented by all output formatters.
type Formatter interface {
	Format(msg Message) ([]byte, error)
}

// PipelineConfig holds the configuration for a Pipeline.
type PipelineConfig struct {
	Filter    FilterConfig
	Format    string
	Writer    io.Writer
}

// FilterConfig carries filter constraints.
type FilterConfig struct {
	Tables  []string
	Actions []string
}

// NewPipeline constructs a Pipeline from config.
func NewPipeline(cfg PipelineConfig) (*Pipeline, error) {
	filter := NewFilter(cfg.Filter.Tables, cfg.Filter.Actions)

	formatter, err := NewFormatter(cfg.Format)
	if err != nil {
		return nil, fmt.Errorf("pipeline: %w", err)
	}

	return &Pipeline{
		filter:    filter,
		formatter: formatter,
		writer:    cfg.Writer,
	}, nil
}

// Run reads messages from in, filters and formats them, writing to the pipeline writer.
func (p *Pipeline) Run(ctx context.Context, in <-chan Message) error {
	for {
		select {
		case msg, ok := <-in:
			if !ok {
				return nil
			}
			if !p.filter.Match(msg) {
				continue
			}
			data, err := p.formatter.Format(msg)
			if err != nil {
				return fmt.Errorf("pipeline: format: %w", err)
			}
			if _, err := p.writer.Write(append(data, '\n')); err != nil {
				return fmt.Errorf("pipeline: write: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
