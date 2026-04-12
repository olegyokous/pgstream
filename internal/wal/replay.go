package wal

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
)

// ReplayConfig holds configuration for the ReplayBuffer.
type ReplayConfig struct {
	// MaxMessages is the maximum number of messages to buffer for replay.
	MaxMessages int
	// TTL is how long a buffered message is retained before being discarded.
	TTL time.Duration
}

// DefaultReplayConfig returns sensible defaults for ReplayConfig.
func DefaultReplayConfig() ReplayConfig {
	return ReplayConfig{
		MaxMessages: 512,
		TTL:         5 * time.Minute,
	}
}

type replayEntry struct {
	msg       Message
	recordedAt time.Time
}

// ReplayBuffer stores recent WAL messages and can replay them to a writer.
type ReplayBuffer struct {
	mu      sync.Mutex
	entries []replayEntry
	cfg     ReplayConfig
	clock   func() time.Time
}

// NewReplayBuffer creates a ReplayBuffer with the given config.
func NewReplayBuffer(cfg ReplayConfig) *ReplayBuffer {
	if cfg.MaxMessages <= 0 {
		cfg.MaxMessages = DefaultReplayConfig().MaxMessages
	}
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultReplayConfig().TTL
	}
	return &ReplayBuffer{
		cfg:   cfg,
		clock: time.Now,
	}
}

// Record stores a message in the replay buffer, evicting oldest when full.
func (r *ReplayBuffer) Record(msg Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) >= r.cfg.MaxMessages {
		r.entries = r.entries[1:]
	}
	r.entries = append(r.entries, replayEntry{msg: msg, recordedAt: r.clock()})
}

// Replay writes all non-expired messages to the given writer using the formatter.
// It returns the number of messages replayed and any write error.
func (r *ReplayBuffer) Replay(ctx context.Context, f *Formatter, w io.Writer) (int, error) {
	r.mu.Lock()
	snap := make([]replayEntry, len(r.entries))
	copy(snap, r.entries)
	r.mu.Unlock()

	now := r.clock()
	count := 0
	for _, e := range snap {
		if now.Sub(e.recordedAt) > r.cfg.TTL {
			continue
		}
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		default:
		}
		line, err := f.Format(e.msg)
		if err != nil {
			return count, fmt.Errorf("replay format: %w", err)
		}
		if _, err := fmt.Fprintln(w, line); err != nil {
			return count, fmt.Errorf("replay write: %w", err)
		}
		count++
	}
	return count, nil
}

// Len returns the current number of buffered messages.
func (r *ReplayBuffer) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// Reset clears all buffered messages.
func (r *ReplayBuffer) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = r.entries[:0]
}

// Evict removes all messages that have exceeded the configured TTL.
// It returns the number of messages evicted. This can be called periodically
// to reclaim memory without discarding still-valid entries.
func (r *ReplayBuffer) Evict() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clock()
	origLen := len(r.entries)
	valid := r.entries[:0]
	for _, e := range r.entries {
		if now.Sub(e.recordedAt) <= r.cfg.TTL {
			valid = append(valid, e)
		}
	}
	r.entries = valid
	return origLen - len(r.entries)
}
