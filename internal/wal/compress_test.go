package wal

import (
	"bytes"
	"compress/gzip"
	"testing"
)

func TestNewCompressor_UnknownFormat(t *testing.T) {
	_, err := NewCompressor(CompressorConfig{Format: "zstd"})
	if err == nil {
		t.Fatal("expected error for unknown format")
	}
}

func TestNewCompressor_KnownFormats(t *testing.T) {
	for _, f := range []CompressFormat{CompressGzip, CompressNone} {
		_, err := NewCompressor(CompressorConfig{Format: f})
		if err != nil {
			t.Fatalf("unexpected error for format %q: %v", f, err)
		}
	}
}

func TestCompressor_NoneIsPassthrough(t *testing.T) {
	c, _ := NewCompressor(CompressorConfig{Format: CompressNone})
	src := []byte("hello world")
	out, err := c.Compress(src)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if !bytes.Equal(src, out) {
		t.Errorf("none compress changed data")
	}
	back, err := c.Decompress(out)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(src, back) {
		t.Errorf("none decompress changed data")
	}
}

func TestCompressor_GzipRoundTrip(t *testing.T) {
	c, _ := NewCompressor(DefaultCompressorConfig())
	src := []byte("pgstream WAL change payload")
	compressed, err := c.Compress(src)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if bytes.Equal(src, compressed) {
		t.Error("expected compressed output to differ from input")
	}
	back, err := c.Decompress(compressed)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(src, back) {
		t.Errorf("round-trip mismatch: got %q, want %q", back, src)
	}
}

func TestCompressor_GzipReducesSize(t *testing.T) {
	c, _ := NewCompressor(CompressorConfig{Format: CompressGzip, Level: gzip.BestCompression})
	src := bytes.Repeat([]byte("aaaa"), 100)
	out, err := c.Compress(src)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if len(out) >= len(src) {
		t.Errorf("expected compressed size %d < original %d", len(out), len(src))
	}
}

func TestCompressor_DecompressInvalidData(t *testing.T) {
	c, _ := NewCompressor(DefaultCompressorConfig())
	_, err := c.Decompress([]byte("not gzip data"))
	if err == nil {
		t.Fatal("expected error decompressing invalid data")
	}
}
