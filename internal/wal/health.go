package wal

import (
	"encoding/json"
	"net/http"
	"time"
)

// HealthStatus represents the current health of the pgstream process.
type HealthStatus struct {
	Status    string    `json:"status"`
	Uptime    string    `json:"uptime"`
	Messages  int64     `json:"messages_processed"`
	Errors    int64     `json:"errors"`
	Filtered  int64     `json:"messages_filtered"`
	Timestamp time.Time `json:"timestamp"`
}

// HealthHandler returns an http.HandlerFunc that serves health check responses
// using a snapshot of the provided Metrics.
func HealthHandler(m *Metrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snap := m.Snapshot()

		status := HealthStatus{
			Status:   "ok",
			Uptime:   snap.Uptime.Round(time.Second).String(),
			Messages: snap.MessagesProcessed,
			Errors:   snap.Errors,
			Filtered: snap.MessagesFiltered,
			Timestamp: time.Now().UTC(),
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(status)
	}
}

// StartHealthServer starts a lightweight HTTP server on the given address
// (e.g. ":9090") that exposes a /health endpoint. It returns immediately and
// runs the server in a background goroutine. Errors from ListenAndServe are
// silently discarded because the health server is best-effort.
func StartHealthServer(addr string, m *Metrics) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", HealthHandler(m))

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() { _ = srv.ListenAndServe() }()
}
