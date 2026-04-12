package wal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestDebouncer_IntegratesWithMessages(t *testing.T) {
	d := NewDebouncer(DebounceConfig{Window: 20 * time.Millisecond, MaxDelay: 200 * time.Millisecond})

	msgs := []*Message{
		{LSN: 10, Table: "orders", Action: "INSERT"},
		{LSN: 11, Table: "orders", Action: "UPDATE"},
		{LSN: 12, Table: "orders", Action: "DELETE"},
	}
	for _, m := range msgs {
		d.Feed(m)
	}

	var got *Message
	select {
	case got = <-d.Out():
	case <-time.After(300 * time.Millisecond):
		t.Fatal("no message received")
	}

	if got.LSN != 12 {
		t.Fatalf("expected coalesced LSN 12, got %d", got.LSN)
	}
}

func TestDebouncer_RunCallbackInvokedForEachFlush(t *testing.T) {
	d := NewDebouncer(DebounceConfig{Window: 15 * time.Millisecond, MaxDelay: 150 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	var count atomic.Int32

	go func() {
		_ = d.Run(ctx, func(m *Message) error {
			count.Add(1)
			return nil
		})
	}()

	// First burst
	for i := uint64(1); i <= 3; i++ {
		d.Feed(debounceMsg(i))
	}
	time.Sleep(60 * time.Millisecond)

	// Second burst after window has settled
	for i := uint64(4); i <= 6; i++ {
		d.Feed(debounceMsg(i))
	}
	time.Sleep(60 * time.Millisecond)

	<-ctx.Done()

	if n := count.Load(); n < 2 {
		t.Fatalf("expected at least 2 callback invocations, got %d", n)
	}
}
