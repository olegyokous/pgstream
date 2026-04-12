package wal

import (
	"context"
	"testing"
	"time"
)

func debounceMsg(lsn uint64) *Message {
	return &Message{LSN: lsn, Table: "events", Action: "INSERT"}
}

func TestDebouncer_DefaultConfigApplied(t *testing.T) {
	d := NewDebouncer(DebounceConfig{})
	if d.cfg.Window != DefaultDebounceConfig().Window {
		t.Fatalf("expected default window %v, got %v", DefaultDebounceConfig().Window, d.cfg.Window)
	}
	if d.cfg.MaxDelay != DefaultDebounceConfig().MaxDelay {
		t.Fatalf("expected default max delay %v, got %v", DefaultDebounceConfig().MaxDelay, d.cfg.MaxDelay)
	}
}

func TestDebouncer_NilMessageIsIgnored(t *testing.T) {
	d := NewDebouncer(DebounceConfig{Window: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond})
	d.Feed(nil)
	select {
	case msg := <-d.Out():
		t.Fatalf("unexpected message: %+v", msg)
	case <-time.After(50 * time.Millisecond):
		// expected: nothing emitted
	}
}

func TestDebouncer_EmitsAfterWindow(t *testing.T) {
	d := NewDebouncer(DebounceConfig{Window: 20 * time.Millisecond, MaxDelay: 500 * time.Millisecond})
	d.Feed(debounceMsg(1))

	select {
	case msg := <-d.Out():
		if msg.LSN != 1 {
			t.Fatalf("expected LSN 1, got %d", msg.LSN)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for debounced message")
	}
}

func TestDebouncer_CoalescesRapidMessages(t *testing.T) {
	d := NewDebouncer(DebounceConfig{Window: 30 * time.Millisecond, MaxDelay: 500 * time.Millisecond})

	for i := uint64(1); i <= 5; i++ {
		d.Feed(debounceMsg(i))
	}

	var received []*Message
	timeout := time.After(200 * time.Millisecond)
drain:
	for {
		select {
		case msg := <-d.Out():
			received = append(received, msg)
		case <-timeout:
			break drain
		}
	}

	if len(received) != 1 {
		t.Fatalf("expected 1 coalesced message, got %d", len(received))
	}
	if received[0].LSN != 5 {
		t.Fatalf("expected last LSN 5, got %d", received[0].LSN)
	}
}

func TestDebouncer_RunStopsOnContextCancel(t *testing.T) {
	d := NewDebouncer(DebounceConfig{Window: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- d.Run(ctx, func(m *Message) error { return nil })
	}()

	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestDebouncer_MaxDelayForcesFlush(t *testing.T) {
	d := NewDebouncer(DebounceConfig{Window: 500 * time.Millisecond, MaxDelay: 30 * time.Millisecond})
	d.Feed(debounceMsg(42))

	select {
	case msg := <-d.Out():
		if msg.LSN != 42 {
			t.Fatalf("expected LSN 42, got %d", msg.LSN)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("max delay did not force a flush")
	}
}
