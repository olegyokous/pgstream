package wal

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

func TestJournal_ConcurrentRecordsAreSafe(t *testing.T) {
	j := NewJournal()
	const workers = 20
	const perWorker = 50

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			for k := 0; k < perWorker; k++ {
				j.Record(journalMsg("tbl", "INSERT", uint64(i*perWorker+k)))
			}
		}(i)
	}
	wg.Wait()

	if got := j.Len(); got != workers*perWorker {
		t.Fatalf("expected %d entries, got %d", workers*perWorker, got)
	}
}

func TestJournal_FlushAndReRecord(t *testing.T) {
	j := NewJournal()
	j.Record(journalMsg("a", "INSERT", 1))
	j.Record(journalMsg("b", "UPDATE", 2))

	var buf bytes.Buffer
	if err := j.Flush(&buf); err != nil {
		t.Fatalf("first flush: %v", err)
	}
	if j.Len() != 0 {
		t.Fatalf("journal not empty after flush")
	}

	// record again and flush a second time
	j.Record(journalMsg("c", "DELETE", 3))
	var buf2 bytes.Buffer
	if err := j.Flush(&buf2); err != nil {
		t.Fatalf("second flush: %v", err)
	}

	if !strings.Contains(buf2.String(), "DELETE") {
		t.Errorf("second flush missing DELETE entry")
	}
	if strings.Contains(buf2.String(), "INSERT") {
		t.Errorf("second flush should not contain first-flush entries")
	}
}
