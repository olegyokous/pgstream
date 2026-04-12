package wal

import (
	"context"
	"sync"
	"time"
)

// DefaultDebounceConfig returns a sensible default debounce configuration.
func DefaultDebounceConfig() DebounceConfig {
	return DebounceConfig{
		Window:   200 * time.Millisecond,
		MaxDelay: 2 * time.Second,
	}
}

// DebounceConfig controls the debounce behaviour.
type DebounceConfig struct {
	// Window is the quiet period after the last event before the callback fires.
	Window time.Duration
	// MaxDelay is the maximum time to wait before forcing a flush regardless of
	// incoming events.
	MaxDelay time.Duration
}

// Debouncer coalesces rapid messages and emits at most one per Window of
// inactivity, or one per MaxDelay under continuous load.
type Debouncer struct {
	cfg     DebounceConfig
	mu      sync.Mutex
	pending *Message
	timer   *time.Timer
	forced  *time.Timer
	out     chan *Message
}

// NewDebouncer creates a Debouncer with the given config.
func NewDebouncer(cfg DebounceConfig) *Debouncer {
	if cfg.Window <= 0 {
		cfg.Window = DefaultDebounceConfig().Window
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = DefaultDebounceConfig().MaxDelay
	}
	return &Debouncer{
		cfg: cfg,
		out: make(chan *Message, 64),
	}
}

// Feed submits a message into the debouncer. The most recent message within
// each debounce window is forwarded to the output channel.
func (d *Debouncer) Feed(msg *Message) {
	if msg == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	d.pending = msg

	if d.timer != nil {
		d.timer.Reset(d.cfg.Window)
	} else {
		d.timer = time.AfterFunc(d.cfg.Window, d.flush)
	}

	if d.forced == nil {
		d.forced = time.AfterFunc(d.cfg.MaxDelay, d.flush)
	}
}

func (d *Debouncer) flush() {
	d.mu.Lock()
	msg := d.pending
	d.pending = nil
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	if d.forced != nil {
		d.forced.Stop()
		d.forced = nil
	}
	d.mu.Unlock()

	if msg != nil {
		select {
		case d.out <- msg:
		default:
		}
	}
}

// Out returns the channel on which debounced messages are emitted.
func (d *Debouncer) Out() <-chan *Message { return d.out }

// Run drains the debouncer output and calls fn for each message until ctx is
// cancelled.
func (d *Debouncer) Run(ctx context.Context, fn func(*Message) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-d.out:
			if err := fn(msg); err != nil {
				return err
			}
		}
	}
}
