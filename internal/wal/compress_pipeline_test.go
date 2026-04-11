package wal

import (
	"encoding/json"
	"testing"
)

// TestCompressor_CompressesFormattedMessage verifies that a JSON-formatted
// WAL message can be compressed and decompressed, preserving the payload.
func TestCompressor_CompressesFormattedMessage(t *testing.T) {
	msg := &Message{
		Schema: "public",
		Table:  "orders",
		Action: "INSERT",
		Columns: []Column{
			{Name: "id", Value: "42"},
			{Name: "total", Value: "99.99"},
		},
	}

	raw, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	c, err := NewCompressor(DefaultCompressorConfig())
	if err != nil {
		t.Fatalf("new compressor: %v", err)
	}

	comp, err := c.Compress(raw)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}

	back, err := c.Decompress(comp)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}

	var got Message
	if err := json.Unmarshal(back, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.Table != msg.Table || got.Action != msg.Action {
		t.Errorf("pipeline mismatch: got %+v, want %+v", got, msg)
	}
	if len(got.Columns) != len(msg.Columns) {
		t.Fatalf("column count mismatch: got %d, want %d", len(got.Columns), len(msg.Columns))
	}
}

// TestCompressor_NoneFormatPreservesBytes ensures the none compressor is
// transparent in a pipeline context.
func TestCompressor_NoneFormatPreservesBytes(t *testing.T) {
	c, _ := NewCompressor(CompressorConfig{Format: CompressNone})

	payload := []byte(`{"schema":"public","table":"users","action":"DELETE"}`)
	out, err := c.Compress(payload)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	if string(out) != string(payload) {
		t.Errorf("none compress mutated payload")
	}
}
