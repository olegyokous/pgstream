package wal

import (
	"bytes"
	"encoding/json"
	"testing"
)

// TestMasker_IntegratesWithFormatter verifies that after masking, the
// formatted JSON output contains the masked value rather than the original.
func TestMasker_IntegratesWithFormatter(t *testing.T) {
	masker, err := NewMasker([]MaskRule{
		{Table: "users", Column: "password", Mode: MaskModeRedact, Replace: "[REDACTED]"},
	})
	if err != nil {
		t.Fatalf("NewMasker: %v", err)
	}

	msg := &Message{
		Table:  "users",
		Action: "INSERT",
		Columns: []Column{
			{Name: "id", Value: int64(1)},
			{Name: "password", Value: "s3cr3t"},
		},
	}

	masked := masker.Apply(msg)

	fmt, err := NewFormatter("json")
	if err != nil {
		t.Fatalf("NewFormatter: %v", err)
	}

	var buf bytes.Buffer
	if err := fmt.Format(masked, &buf); err != nil {
		t.Fatalf("Format: %v", err)
	}

	var out map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	cols, ok := out["columns"].([]interface{})
	if !ok {
		t.Fatal("expected columns array")
	}

	for _, c := range cols {
		cm := c.(map[string]interface{})
		if cm["name"] == "password" {
			if cm["value"] != "[REDACTED]" {
				t.Errorf("expected [REDACTED], got %v", cm["value"])
			}
			return
		}
	}
	t.Error("password column not found in output")
}

// TestMasker_HashColumnInChain verifies hash masking produces a deterministic
// value that is consistent across two calls with the same input.
func TestMasker_HashColumnInChain(t *testing.T) {
	masker, _ := NewMasker([]MaskRule{
		{Table: "*", Column: "email", Mode: MaskModeHash},
	})

	make := func() *Message {
		return &Message{
			Table:  "accounts",
			Action: "UPDATE",
			Columns: []Column{{Name: "email", Value: "user@example.com"}},
		}
	}

	out1 := masker.Apply(make())
	out2 := masker.Apply(make())

	v1 := out1.Columns[0].Value.(string)
	v2 := out2.Columns[0].Value.(string)

	if v1 != v2 {
		t.Errorf("hash not deterministic: %v != %v", v1, v2)
	}
	if len(v1) != 64 {
		t.Errorf("expected 64-char SHA-256 hex, got len %d", len(v1))
	}
}
