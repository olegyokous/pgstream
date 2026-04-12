package wal_test

import (
	"testing"
	"time"

	"pgstream/internal/wal"
)

func BenchmarkJitterer_Apply(b *testing.B) {
	j := wal.NewJitterer(wal.DefaultJitterConfig())
	d := 500 * time.Millisecond
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = j.Apply(d)
	}
}

func BenchmarkJitterer_ApplyParallel(b *testing.B) {
	j := wal.NewJitterer(wal.DefaultJitterConfig())
	d := 500 * time.Millisecond
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = j.Apply(d)
		}
	})
}
