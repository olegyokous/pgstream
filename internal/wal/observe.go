package wal

import (
	"fmt"
	"io"
	"os"
	"time"
)

// ObserverConfig holds configuration for the Observer.
type ObserverConfig struct {
	// Output is the writer to log observations to. Defaults to os.Stderr.
	Output io.Writer
	// Prefix is an optional string prepended to every observation line.
	Prefix string
	// TimestampFormat is the format passed to time.Format. Defaults to time.RFC3339.
	TimestampFormat string
}

// Observer is a pass-through middleware that logs each message it sees
// without modifying it, useful for debugging pipeline stages.
type Observer struct {
	cfg ObserverConfig
	clock func() time.Time
}

// NewObserver returns an Observer with the given config.
// Zero-value fields are replaced with sensible defaults.
func NewObserver(cfg ObserverConfig) *Observer {
	if cfg.Output == nil {
		cfg.Output = os.Stderr
	}
	if cfg.TimestampFormat == "" {
		cfg.TimestampFormat = time.RFC3339
	}
	return &Observer{cfg: cfg, clock: time.Now}
}

// Observe logs the message and returns it unchanged.
// A nil message is passed through silently.
func (o *Observer) Observe(msg *Message) *Message {
	if msg == nil {
		return nil
	}
	ts := o.clock().Format(o.cfg.TimestampFormat)
	prefix := o.cfg.Prefix
	if prefix != "" {
		prefix = prefix + " "
	}
	fmt.Fprintf(
		o.cfg.Output,
		"%s%s action=%s table=%s lsn=%d columns=%d\n",
		prefix,
		ts,
		msg.Action,
		msg.Table,
		msg.LSN,
		len(msg.Columns),
	)
	return msg
}
