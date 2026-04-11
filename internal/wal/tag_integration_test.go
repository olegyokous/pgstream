package wal_test

import (
	"sync"
	"testing"

	"pgstream/internal/wal"
)

func TestTagger_ConcurrentApply(t *testing.T) {
	tagger := wal.NewTagger(wal.Tag{Key: "env", Value: "ci"})
	tagger.AddRule("events", "INSERT", wal.Tag{Key: "stream", Value: "events"})

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			msg := &wal.Message{Table: "events", Action: "INSERT"}
			out := tagger.Apply(msg)
			if out.Meta["env"] != "ci" {
				t.Errorf("missing env tag")
			}
			if out.Meta["stream"] != "events" {
				t.Errorf("missing stream tag")
			}
		}()
	}
	wg.Wait()
}

func TestTagger_StaticAndRuleCombined(t *testing.T) {
	tagger := wal.NewTagger(
		wal.Tag{Key: "region", Value: "eu-west"},
		wal.Tag{Key: "version", Value: "v2"},
	)
	tagger.AddRule("payments", "UPDATE", wal.Tag{Key: "sensitive", Value: "true"})

	msg := &wal.Message{Table: "payments", Action: "UPDATE"}
	out := tagger.Apply(msg)

	expected := map[string]string{
		"region":    "eu-west",
		"version":   "v2",
		"sensitive": "true",
	}
	for k, v := range expected {
		if out.Meta[k] != v {
			t.Errorf("key %s: want %q got %q", k, v, out.Meta[k])
		}
	}
}
