package wal

import (
	"bytes"
	"sync"
	"testing"
)

func TestCompressor_ConcurrentRoundTrips(t *testing.T) {
	c, err := NewCompressor(DefaultCompressorConfig())
	if err != nil {
		t.Fatalf("new compressor: %v", err)
	}

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			src := bytes.Repeat([]byte{byte(n)}, 256)
			comp, err := c.Compress(src)
			if err != nil {
				t.Errorf("goroutine %d compress: %v", n, err)
				return
			}
			back, err := c.Decompress(comp)
			if err != nil {
				t.Errorf("goroutine %d decompress: %v", n, err)
				return
			}
			if !bytes.Equal(src, back) {
				t.Errorf("goroutine %d: round-trip mismatch", n)
			}
		}(i)
	}
	wg.Wait()
}

func TestCompressor_LargePayload(t *testing.T) {
	c, _ := NewCompressor(DefaultCompressorConfig())
	src := bytes.Repeat([]byte("WAL payload data "), 4096) // ~68 KB

	comp, err := c.Compress(src)
	if err != nil {
		t.Fatalf("compress large: %v", err)
	}

	back, err := c.Decompress(comp)
	if err != nil {
		t.Fatalf("decompress large: %v", err)
	}

	if !bytes.Equal(src, back) {
		t.Error("large payload round-trip mismatch")
	}

	if len(comp) >= len(src) {
		t.Errorf("expected compression to reduce size: %d -> %d", len(src), len(comp))
	}
}
