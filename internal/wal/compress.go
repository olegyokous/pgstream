package wal

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// CompressFormat represents a supported compression algorithm.
type CompressFormat string

const (
	CompressGzip CompressFormat = "gzip"
	CompressNone CompressFormat = "none"
)

// CompressorConfig holds configuration for the Compressor.
type CompressorConfig struct {
	Format CompressFormat
	Level  int // only used for gzip; 0 means default
}

// DefaultCompressorConfig returns a config with gzip at default level.
func DefaultCompressorConfig() CompressorConfig {
	return CompressorConfig{
		Format: CompressGzip,
		Level:  gzip.DefaultCompression,
	}
}

// Compressor compresses and decompresses byte slices.
type Compressor struct {
	cfg CompressorConfig
}

// NewCompressor creates a Compressor from the given config.
func NewCompressor(cfg CompressorConfig) (*Compressor, error) {
	switch cfg.Format {
	case CompressGzip, CompressNone:
		return &Compressor{cfg: cfg}, nil
	default:
		return nil, fmt.Errorf("compress: unknown format %q", cfg.Format)
	}
}

// Compress returns a compressed copy of src.
func (c *Compressor) Compress(src []byte) ([]byte, error) {
	if c.cfg.Format == CompressNone {
		return src, nil
	}
	var buf bytes.Buffer
	level := c.cfg.Level
	if level == 0 {
		level = gzip.DefaultCompression
	}
	w, err := gzip.NewWriterLevel(&buf, level)
	if err != nil {
		return nil, fmt.Errorf("compress: gzip writer: %w", err)
	}
	if _, err := w.Write(src); err != nil {
		return nil, fmt.Errorf("compress: write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("compress: close: %w", err)
	}
	return buf.Bytes(), nil
}

// Decompress returns the decompressed contents of src.
func (c *Compressor) Decompress(src []byte) ([]byte, error) {
	if c.cfg.Format == CompressNone {
		return src, nil
	}
	r, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("compress: gzip reader: %w", err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("compress: read: %w", err)
	}
	return out, nil
}
