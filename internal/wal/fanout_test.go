package wal

import (
	"errors"
	"testing"
)

// stubWriter records every message written to it.
type stubWriter struct {
	msgs []*Message
	err  error
}

func (s *stubWriter) Write(msg *Message) error {
	s.msgs = append(s.msgs, msg)
	return s.err
}

func TestFanout_RequiresAtLeastOneWriter(t *testing.T) {
	_, err := NewFanout(nil)
	if err == nil {
		t.Fatal("expected error for empty writers map")
	}
}

func TestFanout_DispatchesToAllWriters(t *testing.T) {
	a, b := &stubWriter{}, &stubWriter{}
	f, err := NewFanout(map[string]Writer{"a": a, "b": b})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msg := &Message{Table: "users", Action: "INSERT"}
	if err := f.Dispatch(msg); err != nil {
		t.Fatalf("unexpected dispatch error: %v", err)
	}

	if len(a.msgs) != 1 || len(b.msgs) != 1 {
		t.Errorf("expected each writer to receive 1 message; got a=%d b=%d", len(a.msgs), len(b.msgs))
	}
}

func TestFanout_ContinuesAfterWriterError(t *testing.T) {
	failing := &stubWriter{err: errors.New("disk full")}
	good := &stubWriter{}

	f, _ := NewFanout(map[string]Writer{"failing": failing, "good": good})

	msg := &Message{Table: "orders", Action: "UPDATE"}
	err := f.Dispatch(msg)

	if err == nil {
		t.Fatal("expected error from failing writer")
	}
	// good writer must still have received the message
	if len(good.msgs) != 1 {
		t.Errorf("good writer should have received the message despite sibling failure")
	}
}

func TestFanout_Len(t *testing.T) {
	f, _ := NewFanout(map[string]Writer{
		"x": &stubWriter{},
		"y": &stubWriter{},
		"z": &stubWriter{},
	})
	if f.Len() != 3 {
		t.Errorf("expected Len()=3, got %d", f.Len())
	}
}

func TestFanout_NoErrorWhenAllSucceed(t *testing.T) {
	f, _ := NewFanout(map[string]Writer{"only": &stubWriter{}})
	if err := f.Dispatch(&Message{}); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}
