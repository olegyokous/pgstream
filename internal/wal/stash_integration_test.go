package wal

import (
	"fmt"
	"sync"
	"testing"
)

func TestStash_ConcurrentPutGet(t *testing.T) {
	s, _ := NewStash(512)
	var wg sync.WaitGroup
	const workers = 32
	const perWorker = 16

	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				key := fmt.Sprintf("w%d-i%d", w, i)
				_ = s.Put(key, &Message{Table: "t", Action: "INSERT"})
				_, _ = s.Get(key)
			}
		}()
	}
	wg.Wait()

	if s.Len() > workers*perWorker {
		t.Errorf("stash len %d exceeds expected upper bound", s.Len())
	}
}

func TestStash_ConcurrentFlushAndPut(t *testing.T) {
	s, _ := NewStash(512)
	var wg sync.WaitGroup

	// Writers
	for w := 0; w < 8; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				key := fmt.Sprintf("w%d-i%d", w, i)
				_ = s.Put(key, &Message{Table: "orders", Action: "UPDATE"})
			}
		}()
	}

	// Concurrent flusher
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			_ = s.Flush()
		}
	}()

	wg.Wait()
	// No assertions needed beyond no data race / panic.
}
