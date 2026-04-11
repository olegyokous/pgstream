package wal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestThrottler_ConcurrentWaiters(t *testing.T) {
	th := NewThrottler(ThrottleConfig{MessagesPerSecond: 500, BurstSize: 20})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var passed int64
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			for {
				if err := th.Wait(ctx); err != nil {
					close(done)
					return
				}
				atomic.AddInt64(&passed, 1)
			}
		}()
	}
	<-done
	if atomic.LoadInt64(&passed) == 0 {
		t.Fatal("expected at least one message to pass through")
	}
}

func TestThrottler_RateIsApproximatelyCorrect(t *testing.T) {
	rate := 200
	th := NewThrottler(ThrottleConfig{MessagesPerSecond: rate, BurstSize: 1})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	var count int
	start := time.Now()
	for {
		if err := th.Wait(ctx); err != nil {
			break
		}
		count++
	}
	elapsed := time.Since(start).Seconds()
	actualRate := float64(count) / elapsed
	// Allow ±50% tolerance for CI timing variance.
	if actualRate < float64(rate)*0.5 || actualRate > float64(rate)*1.5 {
		t.Errorf("rate %.1f msg/s outside expected range [%d, %d]", actualRate, rate/2, rate*3/2)
	}
}
