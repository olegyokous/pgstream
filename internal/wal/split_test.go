package wal

import (
	"testing"
)

func splitMsg(table, action string) *Message {
	return &Message{Table: table, Action: action}
}

func TestNewSplitter_NilPredicateErrors(t *testing.T) {
	_, err := NewSplitter(nil, DefaultSplitterConfig())
	if err == nil {
		t.Fatal("expected error for nil predicate")
	}
}

func TestNewSplitter_DefaultBufferSize(t *testing.T) {
	s, err := NewSplitter(func(*Message) bool { return true }, SplitterConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cap(s.Left) != 64 || cap(s.Right) != 64 {
		t.Errorf("expected buffer size 64, got left=%d right=%d", cap(s.Left), cap(s.Right))
	}
}

func TestSplitter_DispatchNilErrors(t *testing.T) {
	s, _ := NewSplitter(func(*Message) bool { return true }, DefaultSplitterConfig())
	if err := s.Dispatch(nil); err == nil {
		t.Fatal("expected error dispatching nil")
	}
}

func TestSplitter_MatchGoesLeft(t *testing.T) {
	s, _ := NewSplitter(func(m *Message) bool { return m.Table == "users" }, DefaultSplitterConfig())
	msg := splitMsg("users", "INSERT")
	if err := s.Dispatch(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	select {
	case got := <-s.Left:
		if got != msg {
			t.Errorf("expected msg on Left")
		}
	default:
		t.Fatal("expected message on Left channel")
	}
}

func TestSplitter_NoMatchGoesRight(t *testing.T) {
	s, _ := NewSplitter(func(m *Message) bool { return m.Table == "users" }, DefaultSplitterConfig())
	msg := splitMsg("orders", "INSERT")
	if err := s.Dispatch(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	select {
	case got := <-s.Right:
		if got != msg {
			t.Errorf("expected msg on Right")
		}
	default:
		t.Fatal("expected message on Right channel")
	}
}

func TestSplitter_CloseSignalsChannels(t *testing.T) {
	s, _ := NewSplitter(func(*Message) bool { return true }, DefaultSplitterConfig())
	s.Close()
	if _, ok := <-s.Left; ok {
		t.Error("Left channel should be closed")
	}
	if _, ok := <-s.Right; ok {
		t.Error("Right channel should be closed")
	}
}
