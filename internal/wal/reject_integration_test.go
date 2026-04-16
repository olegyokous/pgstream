package wal

import (
	"sync"
	"testing"
)

func TestRejecter_ConcurrentApplyIsSafe(t *testing.T) {
	r, _ := NewRejecter([]RejectRule{
		{Table: "blocked", Reason: "concurrent test"},
	})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			table := "allowed"
			if i%2 == 0 {
				table = "blocked"
			}
			_ = r.Apply(rejectMsg(table, "INSERT"))
		}(i)
	}
	wg.Wait()
}

func TestRejecter_OnlyMatchingMessagesRejected(t *testing.T) {
	r, _ := NewRejecter([]RejectRule{
		{Table: "payments", Action: "DELETE", Reason: "immutable payments"},
	})

	cases := []struct {
		table, action string
		wantErr       bool
	}{
		{"payments", "DELETE", true},
		{"payments", "INSERT", false},
		{"orders", "DELETE", false},
		{"orders", "INSERT", false},
	}

	for _, c := range cases {
		err := r.Apply(rejectMsg(c.table, c.action))
		if c.wantErr && err == nil {
			t.Errorf("%s/%s: expected error", c.table, c.action)
		}
		if !c.wantErr && err != nil {
			t.Errorf("%s/%s: unexpected error: %v", c.table, c.action, err)
		}
	}
}
