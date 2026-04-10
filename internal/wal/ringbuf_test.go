package wal

import (
	"sync"
	"testing"
)

func TestRingBuffer_PushAndPop(t *testing.T) {
	rb := NewRingBuffer(4)
	msg := &Message{Table: "users", Action: "INSERT"}

	if err := rb.Push(msg); err != nil {
		t.Fatalf("unexpected push error: %v", err)
	}
	if rb.Len() != 1 {
		t.Fatalf("expected len 1, got %d", rb.Len())
	}

	out, err := rb.Pop()
	if err != nil {
		t.Fatalf("unexpected pop error: %v", err)
	}
	if out != msg {
		t.Fatalf("expected same message pointer")
	}
	if rb.Len() != 0 {
		t.Fatalf("expected len 0 after pop, got %d", rb.Len())
	}
}

func TestRingBuffer_FullReturnsError(t *testing.T) {
	rb := NewRingBuffer(2)
	msg := &Message{Table: "t", Action: "INSERT"}
	_ = rb.Push(msg)
	_ = rb.Push(msg)

	if err := rb.Push(msg); err != ErrBufferFull {
		t.Fatalf("expected ErrBufferFull, got %v", err)
	}
}

func TestRingBuffer_EmptyReturnsError(t *testing.T) {
	rb := NewRingBuffer(4)
	_, err := rb.Pop()
	if err != ErrBufferEmpty {
		t.Fatalf("expected ErrBufferEmpty, got %v", err)
	}
}

func TestRingBuffer_WrapAround(t *testing.T) {
	rb := NewRingBuffer(3)
	msgs := []*Message{
		{Table: "a"}, {Table: "b"}, {Table: "c"},
	}
	for _, m := range msgs {
		_ = rb.Push(m)
	}
	// Pop one, push one more to trigger wrap
	_, _ = rb.Pop()
	extra := &Message{Table: "d"}
	if err := rb.Push(extra); err != nil {
		t.Fatalf("wrap-around push failed: %v", err)
	}
	if rb.Len() != 3 {
		t.Fatalf("expected len 3, got %d", rb.Len())
	}
}

func TestRingBuffer_DefaultCapacity(t *testing.T) {
	rb := NewRingBuffer(0)
	if rb.Cap() != 64 {
		t.Fatalf("expected default capacity 64, got %d", rb.Cap())
	}
}

func TestRingBuffer_ConcurrentAccess(t *testing.T) {
	rb := NewRingBuffer(128)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = rb.Push(&Message{Table: "concurrent"})
		}()
	}
	wg.Wait()
	if rb.Len() > 128 {
		t.Fatalf("len exceeded capacity: %d", rb.Len())
	}
}
