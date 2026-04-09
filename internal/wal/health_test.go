package wal

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthHandler_ReturnsOK(t *testing.T) {
	m := NewMetrics()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	HealthHandler(m)(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	ct := rec.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Fatalf("expected application/json, got %s", ct)
	}
}

func TestHealthHandler_BodyContainsExpectedFields(t *testing.T) {
	m := NewMetrics()
	m.IncMessages()
	m.IncMessages()
	m.IncFiltered()
	m.IncErrors()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	HealthHandler(m)(rec, req)

	var status HealthStatus
	if err := json.NewDecoder(rec.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Status != "ok" {
		t.Errorf("expected status ok, got %s", status.Status)
	}
	if status.Messages != 2 {
		t.Errorf("expected 2 messages, got %d", status.Messages)
	}
	if status.Filtered != 1 {
		t.Errorf("expected 1 filtered, got %d", status.Filtered)
	}
	if status.Errors != 1 {
		t.Errorf("expected 1 error, got %d", status.Errors)
	}
	if status.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp")
	}
}

func TestHealthHandler_UptimeNonEmpty(t *testing.T) {
	m := NewMetrics()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	HealthHandler(m)(rec, req)

	var status HealthStatus
	if err := json.NewDecoder(rec.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status.Uptime == "" {
		t.Error("expected non-empty uptime")
	}
}
