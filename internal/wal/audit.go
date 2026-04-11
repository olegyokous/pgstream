package wal

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// AuditEntry records a single auditable event from the WAL pipeline.
type AuditEntry struct {
	Timestamp time.Time
	Table     string
	Action    string
	LSN       string
	Message   string
}

// AuditConfig holds configuration for the Auditor.
type AuditConfig struct {
	// Tables restricts auditing to specific tables; empty means all.
	Tables []string
	// Actions restricts auditing to specific actions; empty means all.
	Actions []string
	// Writer is the destination for audit log lines. Defaults to os.Stderr.
	Writer io.Writer
}

// Auditor writes structured audit log entries for matching WAL messages.
type Auditor struct {
	cfg    AuditConfig
	writer io.Writer
	mu     sync.Mutex
	tables map[string]struct{}
	actions map[string]struct{}
}

// NewAuditor constructs an Auditor from the given config.
func NewAuditor(cfg AuditConfig) *Auditor {
	w := cfg.Writer
	if w == nil {
		w = os.Stderr
	}
	a := &Auditor{
		cfg:     cfg,
		writer:  w,
		tables:  make(map[string]struct{}, len(cfg.Tables)),
		actions: make(map[string]struct{}, len(cfg.Actions)),
	}
	for _, t := range cfg.Tables {
		a.tables[t] = struct{}{}
	}
	for _, ac := range cfg.Actions {
		a.actions[ac] = struct{}{}
	}
	return a
}

// Record writes an audit entry for msg if it matches the configured filters.
func (a *Auditor) Record(msg *Message) error {
	if !a.matches(msg) {
		return nil
	}
	entry := fmt.Sprintf("time=%s table=%s action=%s lsn=%s\n",
		time.Now().UTC().Format(time.RFC3339),
		msg.Table,
		msg.Action,
		msg.LSN,
	)
	a.mu.Lock()
	defer a.mu.Unlock()
	_, err := io.WriteString(a.writer, entry)
	return err
}

func (a *Auditor) matches(msg *Message) bool {
	if len(a.tables) > 0 {
		if _, ok := a.tables[msg.Table]; !ok {
			return false
		}
	}
	if len(a.actions) > 0 {
		if _, ok := a.actions[msg.Action]; !ok {
			return false
		}
	}
	return true
}
