package wal

import (
	"sync"
	"testing"
)

func TestSplitter_ConcurrentDispatch(t *testing.T) {
	s, err := NewSplitter(func(m *Message) bool {
		return m.Action == "INSERT"
	}, SplitterConfig{BufferSize: 128})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			action := "INSERT"
			if i%2 == 0 {
				action = "DELETE"
			}
			_ = s.Dispatch(&Message{Table: "t", Action: action})
		}(i)
	}
	wg.Wait()

	left, right := 0, 0
	for len(s.Left) > 0 {
		<-s.Left
		left++
	}
	for len(s.Right) > 0 {
		<-s.Right
		right++
	}
	if left+right != n {
		t.Errorf("expected %d total messages, got %d", n, left+right)
	}
}

func TestSplitter_AllMatchGoLeft(t *testing.T) {
	s, _ := NewSplitter(func(*Message) bool { return true }, DefaultSplitterConfig())
	msgs := []*Message{
		{Table: "a", Action: "INSERT"},
		{Table: "b", Action: "UPDATE"},
		{Table: "c", Action: "DELETE"},
	}
	for _, m := range msgs {
		if err := s.Dispatch(m); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if len(s.Left) != 3 {
		t.Errorf("expected 3 on Left, got %d", len(s.Left))
	}
	if len(s.Right) != 0 {
		t.Errorf("expected 0 on Right, got %d", len(s.Right))
	}
}
