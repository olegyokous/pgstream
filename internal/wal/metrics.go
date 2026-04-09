package wal

import (
	"sync/atomic"
	"time"
)

// Metrics tracks runtime statistics for the WAL pipeline.
type Metrics struct {
	MessagesReceived  atomic.Int64
	MessagesFiltered  atomic.Int64
	MessagesFormatted atomic.Int64
	DecodeErrors      atomic.Int64
	FormatErrors      atomic.Int64
	startTime         time.Time
}

// NewMetrics creates a new Metrics instance with the start time set to now.
func NewMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
	}
}

// Uptime returns the duration since the metrics were initialised.
func (m *Metrics) Uptime() time.Duration {
	return time.Since(m.startTime)
}

// Snapshot returns a point-in-time copy of the current metric values.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		MessagesReceived:  m.MessagesReceived.Load(),
		MessagesFiltered:  m.MessagesFiltered.Load(),
		MessagesFormatted: m.MessagesFormatted.Load(),
		DecodeErrors:      m.DecodeErrors.Load(),
		FormatErrors:      m.FormatErrors.Load(),
		Uptime:            m.Uptime(),
	}
}

// MetricsSnapshot is an immutable point-in-time view of Metrics.
type MetricsSnapshot struct {
	MessagesReceived  int64
	MessagesFiltered  int64
	MessagesFormatted int64
	DecodeErrors      int64
	FormatErrors      int64
	Uptime            time.Duration
}
