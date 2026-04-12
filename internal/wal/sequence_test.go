package wal

import (
	"sync"
	"testing"
)

func seqMsg(table string) *Message {
	return &Message{Table: table, Action: "INSERT"}
}

func TestSequencer_DefaultStartsAtOne(t *testing.T) {
	s, _ := NewSequencer()
	msg, err := s.Stamp(seqMsg("events"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Meta["seq"] != "1" {
		t.Errorf("expected seq=1, got %q", msg.Meta["seq"])
	}
}

func TestSequencer_MonotonicallyIncreases(t *testing.T) {
	s, _ := NewSequencer()
	expected := []string{"1", "2", "3"}
	for _, want := range expected {
		msg, _ := s.Stamp(seqMsg("orders"))
		if msg.Meta["seq"] != want {
			t.Errorf("expected seq=%s, got %q", want, msg.Meta["seq"])
		}
	}
}

func TestSequencer_WithStart(t *testing.T) {
	s, _ := NewSequencer(WithSequencerStart(99))
	msg, _ := s.Stamp(seqMsg("t"))
	if msg.Meta["seq"] != "100" {
		t.Errorf("expected seq=100, got %q", msg.Meta["seq"])
	}
}

func TestSequencer_ScopedTableSkipsOthers(t *testing.T) {
	s, _ := NewSequencer(WithSequencerTable("users"))

	other, _ := s.Stamp(seqMsg("orders"))
	if _, ok := other.Meta["seq"]; ok {
		t.Error("expected no seq tag for non-matching table")
	}

	match, _ := s.Stamp(seqMsg("users"))
	if match.Meta["seq"] != "1" {
		t.Errorf("expected seq=1 for matching table, got %q", match.Meta["seq"])
	}
}

func TestSequencer_NilMessageReturnsError(t *testing.T) {
	s, _ := NewSequencer()
	_, err := s.Stamp(nil)
	if err == nil {
		t.Error("expected error for nil message")
	}
}

func TestSequencer_ResetRestartsCounting(t *testing.T) {
	s, _ := NewSequencer()
	s.Stamp(seqMsg("t"))
	s.Stamp(seqMsg("t"))
	s.Reset()
	if s.Current() != 0 {
		t.Errorf("expected 0 after reset, got %d", s.Current())
	}
	msg, _ := s.Stamp(seqMsg("t"))
	if msg.Meta["seq"] != "1" {
		t.Errorf("expected seq=1 after reset, got %q", msg.Meta["seq"])
	}
}

func TestSequencer_ConcurrentStampsAreUnique(t *testing.T) {
	s, _ := NewSequencer()
	const n = 200
	results := make([]string, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			msg, _ := s.Stamp(seqMsg("t"))
			results[i] = msg.Meta["seq"]
		}()
	}
	wg.Wait()

	seen := make(map[string]bool, n)
	for _, v := range results {
		if seen[v] {
			t.Errorf("duplicate sequence number: %s", v)
		}
		seen[v] = true
	}
}
