package wal

import (
	"fmt"
	"sync"
	"testing"
)

// concurrentWriter is a thread-safe stub used in integration tests.
type concurrentWriter struct {
	mu   sync.Mutex
	msgs []*Message
}

func (c *concurrentWriter) Write(msg *Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, msg)
	return nil
}

func (c *concurrentWriter) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.msgs)
}

func TestFanout_ConcurrentDispatch(t *testing.T) {
	const goroutines = 20

	writers := make(map[string]Writer, 3)
	cws := make([]*concurrentWriter, 3)
	for i := 0; i < 3; i++ {
		cw := &concurrentWriter{}
		cws[i] = cw
		writers[fmt.Sprintf("w%d", i)] = cw
	}

	f, err := NewFanout(writers)
	if err != nil {
		t.Fatalf("NewFanout: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_ = f.Dispatch(&Message{Table: fmt.Sprintf("t%d", n), Action: "INSERT"})
		}(i)
	}
	wg.Wait()

	for i, cw := range cws {
		if cw.Count() != goroutines {
			t.Errorf("writer %d: expected %d messages, got %d", i, goroutines, cw.Count())
		}
	}
}
