package wal

import (
	"context"
	"fmt"
	"io"
	"os"
)

// SinkType identifies the output destination for formatted WAL messages.
type SinkType string

const (
	SinkStdout SinkType = "stdout"
	SinkFile   SinkType = "file"
	SinkNull   SinkType = "null"
)

// Sink writes formatted byte payloads to an output destination.
type Sink interface {
	Write(ctx context.Context, payload []byte) error
	Close() error
}

// NewSink constructs a Sink for the given type and optional target path.
func NewSink(kind SinkType, target string) (Sink, error) {
	switch kind {
	case SinkStdout:
		return &writerSink{w: os.Stdout}, nil
	case SinkFile:
		if target == "" {
			return nil, fmt.Errorf("sink: file target path must not be empty")
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("sink: open file %q: %w", target, err)
		}
		return &writerSink{w: f, closer: f}, nil
	case SinkNull:
		return &nullSink{}, nil
	default:
		return nil, fmt.Errorf("sink: unknown type %q", kind)
	}
}

// writerSink writes each payload followed by a newline.
type writerSink struct {
	w      io.Writer
	closer io.Closer
}

func (s *writerSink) Write(_ context.Context, payload []byte) error {
	_, err := fmt.Fprintf(s.w, "%s\n", payload)
	return err
}

func (s *writerSink) Close() error {
	if s.closer != nil {
		return s.closer.Close()
	}
	return nil
}

// nullSink discards all writes; useful for testing.
type nullSink struct{}

func (n *nullSink) Write(_ context.Context, _ []byte) error { return nil }
func (n *nullSink) Close() error                            { return nil }
