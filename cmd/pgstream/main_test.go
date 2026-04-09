package main

import (
	"testing"
)

func TestSplitCSV_Empty(t *testing.T) {
	result := splitCSV("")
	if result != nil {
		t.Errorf("expected nil for empty string, got %v", result)
	}
}

func TestSplitCSV_Single(t *testing.T) {
	result := splitCSV("users")
	if len(result) != 1 || result[0] != "users" {
		t.Errorf("unexpected result: %v", result)
	}
}

func TestSplitCSV_Multiple(t *testing.T) {
	result := splitCSV("users, orders , products")
	if len(result) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result))
	}
	expected := []string{"users", "orders", "products"}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("index %d: expected %q, got %q", i, v, result[i])
		}
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	cfg := loadConfig()
	if cfg.format == "" {
		t.Error("expected non-empty default format")
	}
	if cfg.slot == "" {
		t.Error("expected non-empty default slot")
	}
	if cfg.publication == "" {
		t.Error("expected non-empty default publication")
	}
}
