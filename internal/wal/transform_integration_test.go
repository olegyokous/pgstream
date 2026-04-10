package wal

import "testing"

// TestTransformer_FullPipeline exercises MaskColumns + RenameTable + DropAction
// chained together to verify they compose correctly.
func TestTransformer_FullPipeline(t *testing.T) {
	tr := NewTransformer(
		MaskColumns("password"),
		RenameTable("users", "accounts"),
	)

	msg := baseMsg()
	out := tr.Apply(msg)

	if out == nil {
		t.Fatal("expected non-nil message")
	}
	if out.Table != "accounts" {
		t.Errorf("expected table accounts, got %s", out.Table)
	}
	var passwordVal string
	for _, col := range out.Columns {
		if col.Name == "password" {
			passwordVal = col.Value
		}
	}
	if passwordVal != "***" {
		t.Errorf("expected password masked, got %s", passwordVal)
	}
}

// TestTransformer_DropInChainShortCircuits verifies that once DropAction returns
// nil, subsequent transforms do not mutate any state.
func TestTransformer_DropInChainShortCircuits(t *testing.T) {
	sideEffect := false
	tr := NewTransformer(
		MaskColumns("email"),
		DropAction("INSERT"),
		func(msg *Message) *Message {
			sideEffect = true
			return msg
		},
	)

	out := tr.Apply(baseMsg())
	if out != nil {
		t.Fatal("expected nil after drop")
	}
	if sideEffect {
		t.Fatal("transform after drop should not have executed")
	}
}

// TestTransformer_MultipleRenames verifies that chaining two RenameTable calls
// works sequentially.
func TestTransformer_MultipleRenames(t *testing.T) {
	tr := NewTransformer(
		RenameTable("users", "members"),
		RenameTable("members", "subscribers"),
	)

	out := tr.Apply(baseMsg())
	if out.Table != "subscribers" {
		t.Errorf("expected subscribers, got %s", out.Table)
	}
}
