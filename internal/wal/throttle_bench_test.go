package wal

import (
	"context"
	"testing"
)

func BenchmarkThrottler_Wait(b *testing.B) {
	th := NewThrottler(ThrottleConfig{MessagesPerSecond: 1_000_000, BurstSize: b.N + 1})
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = th.Wait(ctx)
	}
}

func BenchmarkThrottler_WaitParallel(b *testing.B) {
	th := NewThrottler(ThrottleConfig{MessagesPerSecond: 1_000_000, BurstSize: b.N + 1})
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = th.Wait(ctx)
		}
	})
}
