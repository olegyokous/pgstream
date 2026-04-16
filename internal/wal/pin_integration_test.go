package wal

import (
	"sync"
	"testing"
)

func TestPinner_ConcurrentPinUnpin(t *testing.T) {
	p, _ := NewPinner()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			p.Pin("table_x")
		}()
		go func() {
			defer wg.Done()
			p.Unpin("table_x")
		}()
	}
	wg.Wait()
	// no race — just verify it doesn't panic
}

func TestPinner_ConcurrentIsPinned(t *testing.T) {
	p, _ := NewPinner()
	p.Pin("hot_table")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m := pinMsg("hot_table", "UPDATE")
			if !p.IsPinned(m) {
				t.Errorf("expected hot_table to be pinned")
			}
		}()
	}
	wg.Wait()
}
