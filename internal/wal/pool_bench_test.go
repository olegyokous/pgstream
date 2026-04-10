package wal

import (
	"context"
	"testing"
)

func BenchmarkPool_Submit(b *testing.B) {
	p := NewPool(PoolConfig{Workers: 8, QueueDepth: 512})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Submit(context.Background(), func(ctx context.Context) error {
			return nil
		})
	}
	p.Close()
}

func BenchmarkPool_SubmitParallel(b *testing.B) {
	p := NewPool(PoolConfig{Workers: 8, QueueDepth: 1024})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = p.Submit(context.Background(), func(ctx context.Context) error {
				return nil
			})
		}
	})
	p.Close()
}
