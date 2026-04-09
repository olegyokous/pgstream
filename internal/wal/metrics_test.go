package wal

import (
	"testing"
	"time"
)

func TestNewMetrics_InitialisesZeroCounters(t *testing.T) {
	m := NewMetrics()
	snap := m.Snapshot()

	if snap.MessagesReceived != 0 {
		t.Errorf("expected MessagesReceived=0, got %d", snap.MessagesReceived)
	}
	if snap.MessagesFiltered != 0 {
		t.Errorf("expected MessagesFiltered=0, got %d", snap.MessagesFiltered)
	}
	if snap.MessagesFormatted != 0 {
		t.Errorf("expected MessagesFormatted=0, got %d", snap.MessagesFormatted)
	}
	if snap.DecodeErrors != 0 {
		t.Errorf("expected DecodeErrors=0, got %d", snap.DecodeErrors)
	}
	if snap.FormatErrors != 0 {
		t.Errorf("expected FormatErrors=0, got %d", snap.FormatErrors)
	}
}

func TestMetrics_CountersIncrementCorrectly(t *testing.T) {
	m := NewMetrics()

	m.MessagesReceived.Add(5)
	m.MessagesFiltered.Add(2)
	m.MessagesFormatted.Add(3)
	m.DecodeErrors.Add(1)
	m.FormatErrors.Add(1)

	snap := m.Snapshot()

	if snap.MessagesReceived != 5 {
		t.Errorf("expected MessagesReceived=5, got %d", snap.MessagesReceived)
	}
	if snap.MessagesFiltered != 2 {
		t.Errorf("expected MessagesFiltered=2, got %d", snap.MessagesFiltered)
	}
	if snap.MessagesFormatted != 3 {
		t.Errorf("expected MessagesFormatted=3, got %d", snap.MessagesFormatted)
	}
	if snap.DecodeErrors != 1 {
		t.Errorf("expected DecodeErrors=1, got %d", snap.DecodeErrors)
	}
	if snap.FormatErrors != 1 {
		t.Errorf("expected FormatErrors=1, got %d", snap.FormatErrors)
	}
}

func TestMetrics_UptimeIsPositive(t *testing.T) {
	m := NewMetrics()
	time.Sleep(2 * time.Millisecond)

	if m.Uptime() <= 0 {
		t.Error("expected positive uptime")
	}
}

func TestMetrics_SnapshotIsImmutable(t *testing.T) {
	m := NewMetrics()
	m.MessagesReceived.Add(10)

	snap1 := m.Snapshot()
	m.MessagesReceived.Add(5)
	snap2 := m.Snapshot()

	if snap1.MessagesReceived != 10 {
		t.Errorf("snap1 should be 10, got %d", snap1.MessagesReceived)
	}
	if snap2.MessagesReceived != 15 {
		t.Errorf("snap2 should be 15, got %d", snap2.MessagesReceived)
	}
}
